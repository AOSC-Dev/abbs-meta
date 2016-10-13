#!/bin/ruby

require 'sqlite3'

class Package
	attr_accessor :cat
	attr_accessor :sec
	attr_accessor :def_attr

	def read_att(defines, spec)
		shell = File.read(spec) + "\n"
		shell += File.read(defines) + "\n"
		for arr in $attr_list
			shell += "echo \"#{arr}->\"$#{arr}\n"
		end
		IO.popen(["bash"], "r+") { |f|
			f.puts shell
			f.close_write
			@result = f.read.split("\n")
		}
		for att in @result
			@line = att.split("->")
			self.def_attr[@line[0]] = @line[1] if !@line.nil? && @line.length > 1 && !@line[0].nil? && !@line[0].empty?
		end
	end

	def save
		name = self.def_attr["PKGNAME"]
		return if !name # we assume it is a define for some specific architecture
		section2 = self.def_attr["PKGSEC"]
		description = self.def_attr["PKGDES"]
		version = self.def_attr["VER"]
		release = self.def_attr["REL"]
		
		$db_replaces_package.push "(\"#{name}\", \"#{self.cat}\", \"#{self.sec}\", \"#{section2}\", \"#{version}\", \"#{release}\", \"#{description}\")"
		self.def_attr.each do |a, b|
			$db_replaces_package_spec.push "(\"#{name}\", \"#{a}\", \"#{b}\")"
		end
		for rel in ["PKGDEP", "PKGRECOM", "PKGBREAK", "PKGCONFL", "PKGREP", "BUILDDEP"]
			pkglist = self.def_attr[rel].split() if self.def_attr[rel]
			for pkgn in pkglist
				i = pkgn.index(/[<=>]/)
				if i
					depname = pkgn[0..i - 1]
					depver = pkgn[i..-1]
				else
					depname = pkgn
				end
				$db_replaces_package_dependencies.push "(\"#{name}\", \"#{depname}\", \"#{depver}\", \"#{rel}\")"
			end if pkglist
		end
	end

	def initialize(dir, cat, abbs_pkg)
		if $diff
			IO.popen(["bash"], "r+") { |f|
				f.puts "git --git-dir=#{$git_pool} --work-tree=#{$pool} diff --numstat --minimal #{$obja} #{$objb} -- #{cat}/#{abbs_pkg}/"
				f.close_write
				@change = f.read
			}
			if @change.empty? #|| @defines_change.empty?
				return
			end
		end
		puts "#{cat} : Reading #{abbs_pkg}"
		
		self.def_attr = Hash.new
		@spec_file = File.join(dir, "spec")
		@define_file = File.join(dir, "autobuild/defines")
		self.cat, self.sec = cat.split("-")
		read_att(@define_file, @spec_file)
		save
	end	

end

def setup
	$categories = []
	$pkg_list = []
	$database = ARGV[0]
	$pool = ARGV[1]
	
	$diff = ARGV[2] == "--diff"
	if $diff
		$git_pool = File.join($pool, ".git")
		$obja = ARGV[3]
		$objb = ARGV[4]
	end

	Dir.foreach($pool) do |cat|
		if (cat.start_with?("extra-") || cat.start_with?("base-"))
			$categories.push(cat)
			d = File.join($pool, cat)
			Dir.foreach(d) do |pkg_file|
				pkgd = File.join(d, pkg_file)
				$pkg_list.push({ :pkgd => pkgd, :cat => cat, :pkg_file => pkg_file, :process => false}) if File.exist?(File.join(pkgd,"autobuild"))
			end
		end
	end
end

def worker
	for a in $pkg_list
		if !a[:process]
			a[:process] = true
			pkg = Package.new(a[:pkgd], a[:cat], a[:pkg_file])
			$built_pkg_list.push(pkg)
		end
	end
end

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
	
	$db_replaces_package = []
	$db_replaces_package_spec = []
	$db_replaces_package_dependencies = []
end

setup
$attr_list = ["PKGNAME", "PKGVER", "PKGSEC", "PKGDES", "PKGDEP", "PKGRECOM", "PKGBREAK", "PKGCONFL", "PKGREP","BUILDDEP","VER_NONE", "VER", "SRCTBL", "REL"]
$built_pkg_list = []
threads = (`grep "processor" /proc/cpuinfo | sort -u | wc -l`).to_i
puts threads.to_s + " Threads found"

init_db

i = 0.to_i
while i < threads - 1
	puts "Start Thread"
	Thread.new {worker}
	i+=1
end
puts "Start Main Thread"
worker

puts "Writing database.."
sql = "REPLACE INTO packages (name, category, section, pkg_section, version, release, description) VALUES #{$db_replaces_package.join(", ")}"
$db.execute sql
sql = "REPLACE INTO package_spec (package, key, value) VALUES #{$db_replaces_package_spec.join(", ")}"
$db.execute sql
sql = "REPLACE INTO package_dependencies (package, dependency, version, relationship) VALUES #{$db_replaces_package_dependencies.join(", ")}"
$db.execute sql
puts "Done"
