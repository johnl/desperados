require 'ffi'

module Rados #:nodoc:
  module Lib #:nodoc:
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

  # Initialize the Rados library. Must be called once before any other
  # operations.
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

  # A Rados::Object represents an object in a pool in a cluster.
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

  # Represents a Pool in the cluster.  Use Pool.find to get a pool from the
  # cluster and Pool.create to create new ones.
  class Pool

    # The name of this pool
    attr_reader :name
    # The objects in this pool. See Rados::ObjectCollection.
    attr_reader :objects

    def initialize(name) #:nodoc:
      @name = name
      @objects = ObjectCollection.new(self)
    end

    # Get the named pool from the cluster. Returns a Pool
    # instance. Raises Rados::PoolNotFound if Pool doesn't exist
    def self.find(name)
      p = new(name)
      p.pool
      p
    end

    # Create a new pool in the cluster with the given name. Returns a
    # Pool instance. Raises a RadosError exception if the pool could
    # not be created.
    def self.create(name)
      ret = Lib.rados_create_pool(name)
      if ret < 0
        raise RadosError, "creating pool #{name}"
      else
        new(name)
      end
    end

    # The internal Rados pool data stucture
    def pool #:nodoc:
      return @pool unless @pool.nil?
      @pool_pointer = FFI::MemoryPointer.new :pointer
      ret = Lib.rados_open_pool name, @pool_pointer
      if ret < 0
        raise PoolNotFound, name
      end
      @pool = @pool_pointer.get_pointer(0)
    end

    # Destroy the pool. Raises a RadosError exception if the pool
    # could not be deleted.
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

    # Returns true if the pool as been marked as destroyed since
    # instantiated.
    def destroyed?
      @destroyed == true
    end

    # Write the data <tt>buf</tt> to the object id <tt>oid</tt> in the
    # pool.  Both <tt>oid</tt> and <tt>buf</tt> should be
    # strings. Returns the number of bytes written.  If the write
    # fails then a Rados::WriteError exception is raised. If the data
    # was only partly written then a Rados::ShortWriteError exception
    # is raised.
    #
    # Available options are:
    # * <tt>:size</tt> - Number of bytes to write. Defaults to the size of <tt>buf</tt>
    # * <tt>:offset</tt> - The number of bytes from the beginning of the object to start writing at. Defaults to 0.
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

    # Reads data from the object id <tt>oid</tt> in the pool.  Returns
    # the data as a string. If no object with the oid exists, a
    # Rados::ObjectNotFound exception is raised. If the read fails
    # then a Rados::ReadError exception is raised.
    #
    # Available options are:
    # * <tt>:size</tt> - The amount of data to read. As objects can be huge, it is unlikely that you'll want to load the entire thing into ram, so defaults to 8192 bytes.
    # * <tt>:offset</tt> - The number of bytes from the beginning of the object to start writing at. Defaults to 0.
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

    # Deletes the object id <tt>oid</tt> from the pool.  If the object
    # could not be removed then a Rados::RemoveError exception is
    # raised.
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
