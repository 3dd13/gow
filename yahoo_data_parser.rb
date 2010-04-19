require 'csv'
require 'curl-multi'
require_relative 'stock_daily_price'

module YahooDataParser
  def self.download_last_day_snapshot(last_day_url)
    last_day_value = nil
    
    open_with_error_handled(last_day_url)  { |f|
      last_day_value = StockDailyPrice.new(CSV.parse_line(f.readline))
    }
    last_day_value
  end

  def self.download_history_stock(last_day_stock_price, history_url)
    stock_prices = Array.new
    open_with_error_handled(history_url) { |f|
      skip_header(f)
      
      stock_no = last_day_stock_price.stock_no
      first_history_stock_price = StockDailyPrice.new(CSV.parse_line(f.readline), stock_no)
      if !history_include?(first_history_stock_price, last_day_stock_price)
        stock_prices << last_day_stock_price
      end
      stock_prices << first_history_stock_price
      f.readlines.each { |line|
        stock_prices << StockDailyPrice.new(CSV.parse_line(line), stock_no)
      }
    }
    stock_prices
  end
  
  protected
  
  def self.history_include?(history_stock_price, last_day_stock_price)
    history_stock_price.trade_date == last_day_stock_price.trade_date
  end

  def self.open_with_error_handled(url)
    begin
      open(url) { |f|
        yield f
      }
    rescue OpenURI::HTTPError
 
    end
  end
  
  def self.skip_header(f)
    f.readline
  end 
end
