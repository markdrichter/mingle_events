module MingleEvents
  module Processors
    
    # Writes each event in stream to stdout, mostly for demonstration purposes
    class PutsPublisher < Processor
      
      def process(event) 
 	event.tap { |event| puts "Processing event #{event}" }
      end
    end
  end
end
