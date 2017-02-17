# coding: utf-8
module DataAnon
  module Core

    class TableErrors
      include Utils::Logging

      def initialize table_name
        @table_name = table_name
        @errors = []
      end

      def log_error record, exception
        @errors << { :record => record, :exception => exception}
        puts record.inspect
        puts exception.inspect
        raise "Reached limit of error for a table" if @errors.length > 1
      end

      def errors
        @errors
      end

      def print
        return if @errors.length == 0
        logger.error("Errors while processing table '#{@table_name}':")
        @errors.each do |error|
          logger.error(error[:exception])
          logger.error(error[:exception].backtrace.join("\n\t"))
        end
      end

    end

  end
end
