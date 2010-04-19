require 'rubygems'
require 'active_record'
require 'yaml'
require 'ar-extensions'

dbconfig = YAML::load(File.new(File.join(File.dirname(__FILE__), "config/database.yml")))
ActiveRecord::Base.establish_connection(dbconfig)

class StockDailyPrice < ActiveRecord::Base
  def initialize(stock_values, stock_no = "")
    super()
    if stock_values.nil? || stock_values[2].to_s == 'N/A' || stock_values[4].to_s == 'N/A'
      nil
    else
      case stock_values.size
      when 7
        self.stock_no = stock_no
        self.trade_date = Date.parse(stock_values[0][0..9])
        self.open = stock_values[1]
        self.close = stock_values[2]
        self.high = stock_values[3]
        self.low = stock_values[4]
        self.volume = stock_values[5]
        self.adjusted = true
      when 9
        self.stock_no = stock_values[0]  
        self.close = stock_values[1]
        self.trade_date = Date.strptime(stock_values[2], "%m/%d/%Y")
        self.low = stock_values[5]
        self.high = stock_values[6]      
        self.open = stock_values[7]
        self.volume = stock_values[8]      
        self.adjusted = false
      else
        nil
      end
    end
  end 
end