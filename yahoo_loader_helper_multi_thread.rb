require 'date'
require 'logger'
require_relative 'yahoo_data_parser'
require 'typhoeus'

class YahooLoaderHelper

  include Typhoeus

  def initialize(options)
    @stocks = Array.new
    all_stock_numbers(options).each { |stock_no| 
      @stocks << {
        :stock_no => stock_no,
        :history => "http://ichart.yahoo.com/table.csv?s=#{stock_no}&d=0&e=1&f=2050&g=d&a=0&b=1&c=1980&ignore=.csv",
        :last_day => "http://hk.finance.yahoo.com/d/quotes.csv?s=#{stock_no}&f=sl1d1t1c1ohgv&e=.csv"
      }
    }
  end

  def retrieve_all
    retrieve_last_day_response
    retrieve_history_response
  end

  def retrieve_incremental
    @stocks.each { |stock|
      stock_no = stock[:stock_no]

      if false
        download_missing_stock_prices
      end
    }
  end

  protected

  def all_stock_numbers(options)
    stock_nos = (options[:from]..options[:to]).to_a.map {|num| "#{('%04d' % num).to_s}.HK"}
    stock_nos << "%5EHSI"
  end

  def retrieve_last_day_response
    get_csv(:last_day, false) { |response, stock|
      stock_values = CSV.parse_line(response.body)
      if stock_values && !stock_values.any?{|value| value.to_s == "N/A"}
        stock[:last_day_response] = StockDailyPrice.new(stock_values)
      end
    }
  end

  def retrieve_history_response
    all_stock_prices = Array.new
    lock = Mutex.new

    get_csv(:history, true) { |response, stock|
      stock_no = stock[:stock_no]
      stock_prices = parse_all_stock_data(response, stock[:last_day_response])

      lock.synchronize{
        all_stock_prices += stock_prices
      }
    }

    all_stock_prices
  end

  def get_csv(url_key, filter_last_day)
    hydra = Typhoeus::Hydra.new(:max_concurrency => 50)
    request_pool = Array.new

    @stocks.each { |stock|
      if !filter_last_day || stock[:last_day_response]
        request = Typhoeus::Request.new(stock[url_key])
        request.on_complete do |response|
          yield(response,stock)
        end
        request_pool << request
      end
    }

    request_pool.each{ |request| hydra.queue request}
    hydra.run
    request_pool.each{ |request| request.handled_response}    
  end  

  def parse_all_stock_data(response, last_day_stock_price)
    f = response.body.scan(/[\w\ \,\-\.]+$/)
    stock_prices = Array.new

    stock_no = last_day_stock_price.stock_no

    if stock_price_in_history?(StockDailyPrice.new(CSV.parse_line(f[1])), last_day_stock_price)
      stock_prices << last_day_stock_price
    end

    f[1..f.size-1].each { |line|
      stock_prices << StockDailyPrice.new(CSV.parse_line(line), stock_no)
    }

    stock_prices
  end

  def stock_price_in_history?(history_stock_price, last_day_stock_price)
    history_stock_price.trade_date != last_day_stock_price.trade_date
  end
end

p "Clean up stock_daily_prices: " + Time.now.to_s

StockDailyPrice.delete_all

stock_range = {:from => 1, :to => 100}

loader = YahooLoaderHelper.new(stock_range)

p "Start downloading: #{stock_range.inspect} " + Time.now.to_s

all_stock_prices = loader.retrieve_all
all_stock_prices.each_with_index {|price, index| price.id = index + 1}
p "Finish downloading: " + Time.now.to_s + "\nSaving to DB:"

# fields = [:stock_no, :trade_date, :open, :high, :low, :close, :volume, :adjusted]
StockDailyPrice.import all_stock_prices
p "After db save: " + Time.now.to_s


#"Clean up stock_daily_prices: 2010-04-20 14:09:22 +0800"
#"Start downloading: {:from=>1, :to=>100} 2010-04-20 14:09:22 +0800"
#"Finish downloading: 2010-04-20 14:13:32 +0800\nSaving to DB:"
#"After db save: 2010-04-20 14:22:45 +0800"

