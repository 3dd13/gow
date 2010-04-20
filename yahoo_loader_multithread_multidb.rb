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
    lock = Mutex.new

    get_csv(:history, true) { |response, stock|
      stock_no = stock[:stock_no]
      parse_all_stock_data(response, stock[:last_day_response])
    }
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
    index_start = stock_no == "%5EHSI" ? 500000000 : stock_no[0..3].to_i * 50000

    if stock_price_in_history?(StockDailyPrice.new(CSV.parse_line(f[1])), last_day_stock_price)
      last_day_stock_price.id = index_start
      index_start += 1
      stock_prices << last_day_stock_price
    end
    
    f[1..f.size-1].each_with_index { |line, index|
      stock_prices << StockDailyPrice.new(CSV.parse_line(line), stock_no, index_start + index)
    }

    start = Time.now
    StockDailyPrice.import stock_prices
    p "finish saving to db #{stock_no}: #{Time.now - start}"
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
p "Finish all: " + Time.now.to_s


== BEGIN
"Clean up stock_daily_prices: 2010-04-20 14:40:13 +0800"
"Start downloading: {:from=>1, :to=>100} 2010-04-20 14:40:14 +0800"
"finish saving to db 0012.HK: 3.282888"
"finish saving to db 0021.HK: 2.644956"
"finish saving to db 0028.HK: 2.706294"
"finish saving to db 0003.HK: 3.426228"
"finish saving to db 0004.HK: 3.741749"
"finish saving to db 0006.HK: 3.652848"
"finish saving to db 0011.HK: 3.238389"
"finish saving to db 0015.HK: 3.55156"
"finish saving to db 0018.HK: 3.638393"
"finish saving to db 0020.HK: 3.244737"
"finish saving to db 0022.HK: 2.873179"
"finish saving to db 0025.HK: 3.074157"
"finish saving to db 0026.HK: 3.448205"
"finish saving to db 0031.HK: 1.958895"
"finish saving to db 0043.HK: 2.209187"
"finish saving to db 0001.HK: 3.268558"
"finish saving to db 0002.HK: 3.317476"
"finish saving to db 0005.HK: 3.216817"
"finish saving to db 0007.HK: 4.496486"
"finish saving to db 0008.HK: 3.474394"
"finish saving to db 0009.HK: 3.925027"
"finish saving to db 0010.HK: 3.666417"
"finish saving to db 0014.HK: 3.328959"
"finish saving to db 0016.HK: 3.241216"
"finish saving to db 0017.HK: 3.304958"
"finish saving to db 0019.HK: 3.203529"
"finish saving to db 0023.HK: 3.444417"
"finish saving to db 0027.HK: 3.440647"
"finish saving to db 0029.HK: 3.304762"
"finish saving to db 0037.HK: 1.236257"
"finish saving to db 0013.HK: 3.284369"
"finish saving to db 0024.HK: 5.502169"
"finish saving to db 0030.HK: 3.94064"
"finish saving to db 0038.HK: 3.805979"
"finish saving to db 0041.HK: 3.237978"
"finish saving to db 0042.HK: 3.486762"
"finish saving to db 0045.HK: 3.227741"
"finish saving to db 0046.HK: 3.196842"
"finish saving to db 0050.HK: 3.226696"
"finish saving to db 0036.HK: 2.267195"
"finish saving to db 0039.HK: 4.178928"
"finish saving to db 0044.HK: 3.527489"
"finish saving to db 0051.HK: 3.291738"
"finish saving to db 0052.HK: 3.397016"
"finish saving to db 0091.HK: 0.33792"
"finish saving to db 0090.HK: 0.336152"
"finish saving to db 0040.HK: 5.080857"
"finish saving to db 0047.HK: 4.679327"
"finish saving to db 0049.HK: 4.350798"
"finish saving to db 0073.HK: 0.147434"
"finish saving to db 0072.HK: 0.262273"
"finish saving to db 0070.HK: 0.194254"
"finish saving to db 0034.HK: 3.248455"
"finish saving to db 0035.HK: 3.512693"
"finish saving to db 0095.HK: 1.967806"
"finish saving to db 0032.HK: 4.761116"
"finish saving to db 0099.HK: 3.484294"
"finish saving to db 0079.HK: 1.450283"
"finish saving to db 0076.HK: 1.850154"
"finish saving to db 0054.HK: 0.474994"
"finish saving to db 0100.HK: 3.743762"
"finish saving to db 0094.HK: 3.238131"
"finish saving to db 0093.HK: 3.262382"
"finish saving to db 0089.HK: 3.585956"
"finish saving to db 0077.HK: 2.618624"
"finish saving to db 0056.HK: 1.178511"
"finish saving to db 0055.HK: 1.086268"
"finish saving to db 0098.HK: 4.126802"
"finish saving to db 0097.HK: 3.429864"
"finish saving to db 0092.HK: 3.570299"
"finish saving to db 0087.HK: 3.454313"
"finish saving to db 0086.HK: 3.481838"
"finish saving to db 0085.HK: 3.270979"
"finish saving to db 0084.HK: 3.215419"
"finish saving to db 0082.HK: 3.603596"
"finish saving to db 0081.HK: 3.318992"
"finish saving to db 0075.HK: 3.356651"
"finish saving to db 0067.HK: 2.521443"
"finish saving to db 0083.HK: 3.63535"
"finish saving to db 0078.HK: 3.551789"
"finish saving to db 0074.HK: 3.699801"
"finish saving to db 0068.HK: 3.949809"
"finish saving to db 0063.HK: 3.404432"
"finish saving to db 0088.HK: 4.910476"
"finish saving to db 0069.HK: 3.689361"
"finish saving to db 0064.HK: 3.699826"
"finish saving to db 0062.HK: 3.329677"
"finish saving to db 0058.HK: 3.364072"
"finish saving to db 0071.HK: 5.333502"
"finish saving to db 0065.HK: 5.207903"
"finish saving to db 0061.HK: 5.111356"
"finish saving to db 0053.HK: 3.379309"
"finish saving to db 0066.HK: 4.507958"
"finish saving to db 0060.HK: 4.874691"
"finish saving to db 0057.HK: 4.782003"
"finish saving to db ^HSI: 10.417103"
"Finish all: 2010-04-20 14:47:28 +0800"
== END
