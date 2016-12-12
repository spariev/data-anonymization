require 'parallel'

module DataAnon
  module Parallel
    class Table

      def anonymize tables, options = {}
        ::Parallel.each(tables) do |table|
          begin
            options[:before_hooks][:each].each do |b|
              b.call(table)
            end if options[:before_hooks]
            table.progress_bar_class DataAnon::Utils::ParallelProgressBar
            table.process
            options[:after_hooks][:each].each do |b|
              b.call(table)
            end if options[:after_hooks]

          rescue => e
            logger.error "\n#{e.message} \n #{e.backtrace}"
          end
        end
      end

    end
  end
end
