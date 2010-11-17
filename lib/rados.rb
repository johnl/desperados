require 'ffi'

module Rados
  module Lib
    extend FFI::Library

    ffi_lib 'rados'

    attach_function 'rados_initialize', [ :int, :varargs ], :int
    attach_function 'rados_deinitialize', [], :void
    attach_function 'rados_open_pool', [:string, :pointer], :int
    attach_function 'rados_create_pool', [:string], :int
    attach_function 'rados_write', [:pointer, :string, :off_t, :buffer_in, :size_t], :int
    attach_function 'rados_read', [:pointer, :string, :off_t, :pointer, :size_t], :int
    attach_function 'rados_remove', [:pointer, :string], :int
    attach_function 'rados_delete_pool', [:pointer], :int
  end

  class RadosError < StandardError ; end
  class PoolNotFound < RadosError ; end
  class WriteError < RadosError ; end
  class ShortWriteError < WriteError ; end
  class ReadError < RadosError ; end
  class ObjectNotFound < ReadError ; end

  def self.initialize
    unless @initialized
      ret = Lib.rados_initialize(0)
      if ret < 0 # Blocked by ceph bug #512
        raise RadosError, "Could not initialize rados: #{ret}"
      end
      @initialized = true
    end
    @initialized
  end

  class Object
    attr_reader :id, :position, :pool

    def initialize(attributes = {})
      @id = attributes[:id]
      @pool = attributes[:pool]
      @position = 0
    end

    def write(buf)
      @position += pool.write(id, buf, :offset => @position)
      self
    end

    def read
      buf = pool.read(id, :offset => @position)
      if buf.size == 0
        nil
      else
        @position += buf.size
        buf
      end
    end

    def seek(amount, whence = IO::SEEK_SET)
      case whence
      when IO::SEEK_SET
        @position = amount
      when IO::SEEK_CUR
        @position += amount
      end
    end

  end

  class ObjectCollection

    attr_reader :pool

    def initialize(pool)
      @pool = pool
    end

    def find(oid)
      Object.new(:id => oid, :pool => pool)
    end

    def new(oid = nil)
      Object.new(:id => oid, :pool => pool)
    end

  end

  class Pool

    attr_reader :name, :objects

    def initialize(name)
      @name = name
      @objects = ObjectCollection.new(self)
    end

    def self.find(name)
      p = new(name)
      p.pool
      p
    end

    def self.create(name)
      ret = Lib.rados_create_pool(name)
      if ret < 0
        raise RadosError, "creating pool #{name}"
      else
        new(name)
      end
    end

    def pool
      return @pool unless @pool.nil?
      @pool_pointer = FFI::MemoryPointer.new :pointer
      ret = Lib.rados_open_pool name, @pool_pointer
      if ret < 0
        raise PoolNotFound, name
      end
      @pool = @pool_pointer.get_pointer(0)
    end

    def destroy
      sanity_check "destroy already"
      ret = Lib.rados_delete_pool(pool)
      if ret < 0
        raise RadosError, "deleting pool #{name}"
      else
        @destroyed = true
        self
      end
    end

    def destroyed?
      @destroyed == true
    end

    def write(oid, buf, options = {})
      sanity_check "write to"
      offset = options.fetch(:offset, 0)
      len = options.fetch(:size, buf.size)
      ret = Lib.rados_write(pool, oid, offset, buf, len)
      if ret < 0
        raise WriteError, "writing #{len} bytes at offset #{offset} to #{oid} in pool #{name}: #{ret}"
      elsif ret < len
        raise ShortWriteError, "writing #{len} bytes to pool #{name} only wrote #{ret}"
      end
      ret
    end

    def read(oid, options = {})
      sanity_check "read from"
      offset = options.fetch(:offset, 0)
      len = options.fetch(:size, 8192)
      buf = FFI::MemoryPointer.new :char, len
      ret = Lib.rados_read(pool, oid, offset, buf, len)
      if ret == -2
        raise ObjectNotFound, "reading from '#{oid}' in pool #{name}"
      elsif ret < 0
        raise ReadError, "reading #{len} bytes at offset #{offset} from #{oid} in pool #{name}: #{ret}"
      end
      buf.read_string(ret)
    end

    def remove(oid)
      sanity_check "remove from"
      ret = Lib.rados_remove(pool, oid)
      if ret < 0
        raise RemoveError, "removing #{oid} from pool #{name}"
      end
      true
    end

    private

    def sanity_check(action)
      raise RadosError, "attempt to #{action} destroyed pool #{name}" if destroyed?
    end      
  end
end
