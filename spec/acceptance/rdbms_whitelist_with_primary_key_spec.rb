require "spec_helper"

describe "End 2 End RDBMS Whitelist Acceptance Test using SQLite database" do

  source_connection_spec = {:adapter => 'sqlite3', :database => 'tmp/customer.sqlite'}
  dest_connection_spec = {:adapter => 'sqlite3', :database => 'tmp/customer-dest.sqlite'}

  before(:each) do
    CustomerSample.clean
    CustomerSample.create_schema source_connection_spec
    CustomerSample.insert_record source_connection_spec, CustomerSample::SAMPLE_DATA[0]

    CustomerSample.create_schema dest_connection_spec
  end

  it "should anonymize customer table record " do

    database "Customer" do
      strategy DataAnon::Strategy::Whitelist
      source_db source_connection_spec
      destination_db dest_connection_spec

      table 'customers' do
        primary_key 'cust_id'
        batch_size 1

        whitelist 'cust_id', 'address', 'zipcode', 'blog_url'
        anonymize('first_name').using FieldStrategy::RandomFirstName.new
        anonymize('last_name').using FieldStrategy::RandomLastName.new
        anonymize('state').using FieldStrategy::SelectFromList.new(['Gujrat','Karnataka'])
        anonymize('phone').using FieldStrategy::RandomPhoneNumber.new
        anonymize('email').using FieldStrategy::StringTemplate.new('test+#{row_number}@gmail.com')
        anonymize 'terms_n_condition', 'age', 'longitude'
        anonymize('latitude').using FieldStrategy::RandomFloatDelta.new(2.0)
      end
    end

    DataAnon::Utils::DestinationDatabase.establish_connection dest_connection_spec
    dest_table = DataAnon::Utils::DestinationTable.create 'customers'
    new_rec = dest_table.where("cust_id" => CustomerSample::SAMPLE_DATA[0][:cust_id]).first
    new_rec.first_name.should_not be("Sunit")
    new_rec.last_name.should_not be("Parekh")
    new_rec.birth_date.should_not be(Date.new(1977,7,8))
    new_rec.address.should == 'F 501 Shanti Nagar'
    ['Gujrat','Karnataka'].should include(new_rec.state)
    new_rec.zipcode.should == '411048'
    new_rec.phone.should_not be "9923700662"
    new_rec.email.should == 'test+1@gmail.com'
    [true,false].should include(new_rec.terms_n_condition)
    new_rec.age.should be_between(0,100)
    new_rec.latitude.should be_between( 38.689060, 42.689060)
    new_rec.longitude.should be_between( -84.044636, -64.044636)

  end

  it "should filter records with the given condition" do
    CustomerSample.insert_record source_connection_spec, CustomerSample::SAMPLE_DATA[1]

    database "Customer" do
      strategy DataAnon::Strategy::Whitelist
      source_db source_connection_spec
      destination_db dest_connection_spec

      table 'customers' do
        primary_key 'cust_id'
        batch_size 1
        filter {|rel| rel.where('cust_id > ?', 100)}
        whitelist 'cust_id', 'address', 'zipcode', 'first_name'
      end
    end

    DataAnon::Utils::DestinationDatabase.establish_connection dest_connection_spec
    dest_table = DataAnon::Utils::DestinationTable.create 'customers'
    dest_table.count.should == 1
    new_rec = dest_table.where("cust_id" => CustomerSample::SAMPLE_DATA[1][:cust_id]).first
    new_rec.first_name.should == 'Rohit'
  end
end
