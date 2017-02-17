module DataAnon
  module Core

    class Database
      include Utils::Logging

      def initialize name
        @name = name
        @strategy = DataAnon::Strategy::Whitelist
        @before_hooks = {all: [], each: []}
        @after_hooks = {all: [], each: []}
        @user_defaults = {}
        @tables = []
        @execution_strategy = DataAnon::Core::Sequential
        ENV['parallel_execution'] = 'false'
        I18n.enforce_available_locales = false
      end

      def strategy strategy
        @strategy = strategy
      end

      def execution_strategy execution_strategy
        @execution_strategy = execution_strategy
        ENV['parallel_execution'] = 'true' if execution_strategy == DataAnon::Parallel::Table
      end

      def source_db connection_spec
        @source_database = connection_spec
        fail 'Cannot connect to the Source DB' unless test_connection(DataAnon::Utils::SourceDatabase, connection_spec)
      end

      def destination_db connection_spec
        @destination_database = connection_spec
        fail 'Cannot connect to the Destination DB' unless test_connection(DataAnon::Utils::DestinationDatabase, connection_spec)
        end

      def before(type, &block)
        @before_hooks[type] << block
      end

      def after(type, &block)
        @after_hooks[type] << block
      end

      def default_field_strategies default_strategies
        @user_defaults = default_strategies
      end

      def table (name, &block)
        table = @strategy.new(@source_database, @destination_database, name, @user_defaults).process_fields(&block)
        @tables << table
      end
      alias :collection :table

      def anonymize
        begin
          @before_hooks[:all].each do |b|
            b.call(table)
          end
          @execution_strategy.new.anonymize @tables,
                                            before_hooks: @before_hooks, after_hooks: @after_hooks
          @after_hooks[:all].each do |b|
            b.call(table)
          end

        rescue => e
          logger.error "\n#{e.message} \n #{e.backtrace}"
        end
        if @strategy.whitelist?
          @tables.each do |table|
            if table.fields_missing_strategy.present?
              logger.info('Fields missing the anonymization strategy:')
              table.fields_missing_strategy.print
            end
          end
        end

        @tables.each { |table| table.errors.print }
      end

      private

      def test_connection(db_class, conn_spec)
        db_class.establish_connection conn_spec
        db_class.connection_pool.with_connection { |con| con.active? } rescue false
      end
    end

    class Sequential
      def anonymize tables, options = {}
        tables.each do |table|
          begin
            options[:before_hooks][:each].each do |b|
              b.call(table)
            end if options[:before_hooks]
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
