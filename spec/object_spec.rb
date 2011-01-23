require File.join(File.dirname(__FILE__), '../lib/rados')
include Rados

describe Object do
  def a_pool
    mock("pool")
  end

  it "should take id and pool options" do
    pool = a_pool
    o = Rados::Object.new(:id => "myobj", :pool => pool)
    o.id.should == "myobj"
    o.pool.should == pool
  end

  it "should write to the pool and increase the position" do
    pool = a_pool
    pool.should_receive(:write).with("myobj", "data", :offset => 0).and_return(4)
    o = Rados::Object.new(:id => "myobj", :pool => pool)
    o.position.should == 0
    o.write("data")
    o.position.should == 4
  end

  it "should read from the current position" do
    pool = a_pool
    pool.should_receive(:write).and_return(4)
    pool.should_receive(:read).with("myobj", { :offset => 4 }).and_return("moredata")
    o = Rados::Object.new(:id => "myobj", :pool => pool)
    o.write("data")
    o.read.should == "moredata"
    o.position.should == 12
  end

  it "should seek to the given position in IO:SEEK_SET mode by default" do
    o = Rados::Object.new(:id => "myobj")
    o.position.should == 0
    o.seek(31337)
    o.position.should == 31337
  end

  it "should seek by the given amount in IO:SEEK_CUR mode" do
    o = Rados::Object.new(:id => "myobj")
    o.position.should == 0
    o.seek(100, IO::SEEK_CUR)
    o.seek(100, IO::SEEK_CUR)
    o.position.should == 200
    o.seek(-50, IO::SEEK_CUR)
    o.position.should == 150
  end


end
