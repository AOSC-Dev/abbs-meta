#!/bin/ruby

require 'sqlite3'

class Package
	attr_accessor :cat
	attr_accessor :def_attr

	def read_att(defines, spec)
		shell = File.read(spec) + "\n"
		shell += File.read(defines) + "\n"
		for arr in $attr_list
			shell += "echo #{arr}=$#{arr}\n"
		end
		IO.popen(shell) { |f| @result = f.read.split("\n")}
		for att in @result
			@line = att.split("=")
			self.def_attr[@line[0]] = @line[1] if !@line.nil? && @line.length > 1 && !@line[0].nil? && !@line[0].empty?
		end
	end

	def save
		
	end

	def initialize(dir, cat, abbs_pkg)
		puts "#{cat} : Reading #{abbs_pkg}"
		self.def_attr = Hash.new
		@spec_file = File.join(dir, "spec")
		@define_file = File.join(dir, "autobuild/defines")
		self.cat = cat
		read_att(@define_file, @spec_file)
	end	

end

$pool = "/usr/lib/abbs/repo"

def setup
	$categories = []
	$pkg_list = []
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
	$db = SQLite3::Database.new "abbs.db"
end

setup
$attr_list = ["PKGNAME", "PKGVER", "PKGSEC", "PKGDES", "PKGDEP", "PKGRECOM", "PKGBREAK", "PKGCONFL", "PKGREP","BUILDDEP","VER_NONE", "VER", "SRCTBL", "REL"]
$built_pkg_list = []
threads = (`grep "processor" /proc/cpuinfo | sort -u | wc -l`).to_i
puts threads.to_s + " Threads found"

i = 0.to_i
while i < threads - 1
	puts "Start Thread"
	Thread.new {worker}
	i+=1
end
puts "Start Main Thread"
worker

puts "Writing database.."
init_db
for pkg in $built_pkg_list
	pkg.save
end
