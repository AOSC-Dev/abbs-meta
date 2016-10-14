#!/bin/ruby

require 'sqlite3'

# ==============================================================================
#  Package class for each package
# ==============================================================================
class Package
	attr_accessor :cat
	attr_accessor :sec
	attr_accessor :def_attr

	# Read attributes of a package
	#  From spec and defines
	def read_att(defines, spec)
		# conbine those two files
		shell = File.read(spec) + "\n"
		shell += File.read(defines) + "\n"
		# get the attributes by echo
		for arr in $attr_list
			shell += "echo \"#{arr}->\"$#{arr}\n"
		end
		# execute
		IO.popen(["bash"], "r+") { |f|
			f.puts shell
			f.close_write
			@result = f.read.split("\n")
		}
		# Get the attributes from execute result
		for att in @result
			@line = att.split("->")
			self.def_attr[@line[0]] = @line[1] if !@line.nil? && @line.length > 1 && !@line[0].nil? && !@line[0].empty?
		end
	end

	# Flush the data to sql database
	#  - Preprocess/Prepair the data
	def save
		# basic variables
		name = self.def_attr["PKGNAME"]
		return if !name # we assume it is a define for some specific architecture
		section2 = self.def_attr["PKGSEC"]
		description = self.def_attr["PKGDES"]
		version = self.def_attr["VER"]
		release = self.def_attr["REL"]
		
		# Push the data into SQL data list
		# Package
		$db_replaces_package.push "(\"#{name}\", \"#{self.cat}\", \"#{self.sec}\", \"#{section2}\", \"#{version}\", \"#{release}\", \"#{description}\")"
		# Package spec
		self.def_attr.each do |a, b|
			$db_replaces_package_spec.push "(\"#{name}\", \"#{a}\", \"#{b}\")"
		end
		# Package dependencies
		for rel in ["PKGDEP", "PKGRECOM", "PKGBREAK", "PKGCONFL", "PKGREP", "BUILDDEP"]
			# For each relationship..
			pkglist = self.def_attr[rel].split() if self.def_attr[rel]
			for pkgn in pkglist
				# get version requirement, if it has one
				i = pkgn.index(/[<=>]/)
				if i
					depname = pkgn[0..i - 1]
					depver = pkgn[i..-1]
				else
					depname = pkgn
				end
				# push the data
				$db_replaces_package_dependencies.push "(\"#{name}\", \"#{depname}\", \"#{depver}\", \"#{rel}\")"
			end if pkglist # If the pkglist exist, do the write out
		end
	end

	def initialize(dir, cat, abbs_pkg, defines)
		# return if Diff is open and the package doesn't change
		return if $diff && $total_diff.index("#{cat}/#{abbs_pkg}").nil?
		
		puts "#{cat} : Reading #{abbs_pkg}"
		
		self.def_attr = Hash.new # def_attr we use Hash, faster & better
		@spec_file = File.join(dir, "spec")
		@define_file = File.join(dir, defines)
		self.cat, self.sec = cat.split("-")
		# read and save
		read_att(@define_file, @spec_file)
		save
	end	

end

# ==============================================================================
#  Program setup
#    - Configs
#    - Git Diff
#    - Scan available package
# ==============================================================================
def setup
	# read parameters from ARGV
	$categories = [] # categories list
	$pkg_list = [] # work list
	$database = ARGV[0]
	$pool = ARGV[1]
	
	# Setup Git Diff functionality
	$diff = (ARGV.include? "--diff")
	puts $diff
	if $diff
		# Basic git repo config
		$git_pool = File.join($pool, ".git")
		# Read in comparable objectes
		for p in ARGV
			if p.start_with?("--objA=")
				$obja = p[7..-1].delete("\"\'")
				puts $obja
			end
			if p.start_with?("--objB=")
				$objb = p[7..-1].delete("\"\'")
				puts $objb
			end
		end
		# Use git diff to get output
		IO.popen(["bash"], "r+") { |f|
			f.puts "git --git-dir=#{$git_pool} --work-tree=#{$pool} diff --name-status #{$obja} #{$objb}"
			f.close_write
			# Store all different files for faster querying
			$total_diff = f.read
		}
	end

	# Scan available packages
	Dir.foreach($pool) do |cat|
		# Find the categories
		if (cat.start_with?("extra-") || cat.start_with?("base-"))
			$categories.push(cat)
			# Find directory in catagories
			d = File.join($pool, cat)
			Dir.foreach(d) do |pkg_file|
				pkgd = File.join(d, pkg_file)
				# Find the subdir which contain a define
				Dir.foreach(pkgd) do |defines|
					# Add that package to work list
					$pkg_list.push({ :pkgd => pkgd, :cat => cat, :pkg_file => pkg_file, :defines => "#{defines}/defines", :process => false}) if File.exist?(File.join(pkgd,"#{defines}/defines"))
				end
			end
		end
	end
end

# ==============================================================================
#  Working thread
#    - For scanning the tree faster
# ==============================================================================
def worker
	for a in $pkg_list
		if !a[:process]
			a[:process] = true
			pkg = Package.new(a[:pkgd], a[:cat], a[:pkg_file], a[:defines])
			$built_pkg_list.push(pkg)
		end
	end
end

# ==============================================================================
#  Initialize the database
# ==============================================================================
def init_db
	$db = SQLite3::Database.new $database
	$db.execute <<-SQL
		CREATE TABLE IF NOT EXISTS packages (
		  name TEXT PRIMARY KEY,
		  category TEXT,
		  section TEXT,
		  pkg_section TEXT,
		  version TEXT,
		  release TEXT,
		  description TEXT
		)
	SQL
    
	$db.execute <<-SQL
		CREATE TABLE IF NOT EXISTS package_spec (
		  package TEXT,
		  key TEXT,
		  value TEXT,
		  PRIMARY KEY (package, key)
		)
	SQL
    
	$db.execute <<-SQL
		CREATE TABLE IF NOT EXISTS package_dependencies (
		  package TEXT,
		  dependency TEXT,
		  version TEXT,
		  relationship TEXT,
		  PRIMARY KEY (package, dependency, relationship),
		  FOREIGN KEY(package) REFERENCES packages(name)
		)
	SQL
    
	$db.execute <<-SQL
		CREATE INDEX IF NOT EXISTS idx_package_dependencies
		  ON package_dependencies (package)
	SQL
	
	# prepare the list of database output
	$db_replaces_package = []
	$db_replaces_package_spec = []
	$db_replaces_package_dependencies = []
end

# ==============================================================================
#  MAIN - Everything starts here
# ==============================================================================
setup
$attr_list = ["PKGNAME", "PKGVER", "PKGSEC", "PKGDES", "PKGDEP", "PKGRECOM", "PKGBREAK", "PKGCONFL", "PKGREP","BUILDDEP","VER_NONE", "VER", "SRCTBL", "REL"]
$built_pkg_list = []

init_db

# Check availabe threads
threads = (`grep "processor" /proc/cpuinfo | sort -u | wc -l`).to_i
puts threads.to_s + " Threads found"
# Create threads
i = 0.to_i
while i < threads
	puts "Start Thread"
	Thread.new {worker}
	i+=1
end
puts "Start Main Thread"
worker

# Get data from package to the outside
puts "Writing database.."
# package
sql = "REPLACE INTO packages (name, category, section, pkg_section, version, release, description) VALUES #{$db_replaces_package.join(", ")}"
$db.execute sql if !$db_replaces_package.empty?
# package spec
sql = "REPLACE INTO package_spec (package, key, value) VALUES #{$db_replaces_package_spec.join(", ")}"
$db.execute sql if !$db_replaces_package_spec.empty?
# package dependencies
sql = "REPLACE INTO package_dependencies (package, dependency, version, relationship) VALUES #{$db_replaces_package_dependencies.join(", ")}"
$db.execute sql if !$db_replaces_package_dependencies.empty?
puts "Done"
