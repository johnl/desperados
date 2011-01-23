require File.join(File.dirname(__FILE__), '../lib/rados')
include Rados

describe ObjectCollection do
  def a_pool
    mock("pool")
  end

  it "should take pool as an option" do
    pool = a_pool
    oc = Rados::ObjectCollection.new(pool)
    oc.pool.should == pool
  end

  describe "new" do
    it "should return an new Rados::Object with the given id and pool" do
      pool = a_pool
      oc = Rados::ObjectCollection.new(pool)
      o = oc.new("myobj")
      o.pool.should == pool
      o.id.should == "myobj"
    end
  end

  describe "find" do
    it "should return a new Rados::Object with the given id and pool" do
      pool = a_pool
      oc = Rados::ObjectCollection.new(pool)
      o = oc.find("myobj")
      o.pool.should == pool
      o.id.should == "myobj"
    end
  end
end
