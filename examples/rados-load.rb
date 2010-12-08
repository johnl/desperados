#!/usr/bin/env ruby

require File.dirname(__FILE__) + '/../lib/rados'
require 'logger'
require 'benchmark'

log = Logger.new STDERR
include Rados

Rados::initialize

max_objects = 10000

obj_size = 8192

loop do
  pool_name = "rados-load-#{rand(0xffffff).to_s(36)}"
  log.info "Creating pool #{pool_name}"
  pool = Pool.create pool_name
  log.info "Writing #{max_objects} objects"
  bm = Benchmark.measure do 
    max_objects.times do |i|
      pool.write i.to_s, (i * obj_size).to_s # FIXME: shouldn't have to to_s
    end
  end
  log.info "%.2f objects/second" % (max_objects / bm.real)
  log.info "Reading back #{max_objects} random objects"
  bm = Benchmark::measure do
    max_objects.times do
      i = rand(max_objects)
      data = pool.read i.to_s
      if data != (i * obj_size).to_s
        logger.warn "data in oid #{i} in pool #{pool.name} did not match written"
      end
    end
  end
  log.info "%.2f objects/second" % (max_objects / bm.real)
  log.info "Destroying pool #{pool.name}"
  pool.destroy
end
