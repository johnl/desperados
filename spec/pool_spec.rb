require File.join(File.dirname(__FILE__), '../lib/rados')
include Rados

describe Pool do
  def a_pool_name #:nodoc:
    "pool-#{rand(0xffffffff).to_s(36)}"
  end

  def an_oid #:nodoc:
    "oid-#{rand(0xffffffff).to_s(36)}"
  end

  def a_pool #:nodoc:
    Pool.create(a_pool_name)
  end

  before(:all) do
    Rados::initialize
  end

  describe "#find" do
    it "should raise an error when finding a non-existant pool" do
      lambda { Pool.find("no existy") }.should raise_error Rados::PoolNotFound
    end
  end

  describe "#create" do
    it "should create and return a pool" do
      name = a_pool_name
      pool = Pool.create name
      pool.name.should == name
      Pool.find(name).name.should == name
      pool.destroy
    end
  end

  describe "#destroy" do
    it "should destroy an existing pool" do
      pool = a_pool
      pool.destroy.should == pool
      pool.destroyed?.should == true
      lambda { Pool.find(pool.name) }.should raise_error Rados::PoolNotFound
    end
  end

  describe "#write" do
    before(:all) do
      @pool = a_pool
    end

    after(:all) do
      @pool.destroy
    end

    it "should return the number of bytes written" do
      oid = an_oid
      @pool.write(oid, "hello world").should == "hello world".size
      @pool.read(oid).should == "hello world"
    end

    it "should write strings with null characters in them" do
      oid = an_oid
      @pool.write(oid, "1234\0006789", :size => 9).should == 9
      buf = @pool.read(oid)
      buf.should == "1234\0006789"
      buf.size.should == 9
      buf[4].ord.should == 0
    end

    it "should write at a specified offset" do
      oid = an_oid
      @pool.write(oid, "123456789")
      @pool.write(oid, "abcde", :offset => 3)
      @pool.read(oid).should == "123abcde9"
    end

    it "should write the specified size" do
      oid = an_oid
      @pool.write(oid, "123456789")
      @pool.write(oid, "aaaaaaaaa", :size => 5)
      @pool.read(oid).should == "aaaaa6789"
    end
  end

  describe "#read" do
    before(:all) do
      @pool = a_pool
    end

    after(:all) do
      @pool.destroy
    end

    it "should return a string with the data read" do
      oid = an_oid
      @pool.write(oid, "hello world")
      @pool.read(oid).should == "hello world"
    end

    it "should raise ObjectNotFound when reading an oid that doesn't exist" do
      lambda { @pool.read(an_oid) }.should raise_error Rados::ObjectNotFound
    end

    it "should read from the specified offset" do
      oid = an_oid
      @pool.write(oid, "123456789")
      @pool.read(oid, :offset => 3).should == "456789"
    end

    it "should read the specified size" do
      oid = an_oid
      @pool.write(oid, "123456789")
      @pool.read(oid, :size => 5).should == "12345"
    end
  end

  describe "#remove" do
    before(:all) do
      @pool = a_pool
    end

    after(:all) do
      @pool.destroy
    end

    it "should remove the specified oid" do
      oid = an_oid
      @pool.write(oid, "hello world")
      @pool.remove(oid)
      lambda { @pool.read(oid) }.should raise_error Rados::ObjectNotFound
    end

  end    
end
