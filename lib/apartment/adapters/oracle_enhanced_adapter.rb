require 'apartment/adapters/abstract_adapter'

module Apartment
  module Tenant

    def self.oracle_enhanced_adapter(config)
      Apartment.use_schemas ?
        Adapters::OracleEnhancedSchemaAdapter.new(config) :
        Adapters::OracleEnhancedAdapter.new(config)
    end
  end

  module Adapters
    class OracleEnhancedAdapter < AbstractAdapter

      def initialize(config)
        super

        @default_tenant = config[:username]
      end

    protected

      def rescue_from
        Oracle::Error
      end
    end

    class OracleEnhancedSchemaAdapter < AbstractAdapter
      def initialize(config)
        super

        @default_tenant = config[:username]
        reset
      end

      #   Reset current tenant to the default_tenant
      #
      def reset
        Apartment.connection.execute "ALTER SESSION SET CURRENT_SCHEMA = #{default_tenant}"
      end

    protected

      #   Connect to new tenant
      #
      def connect_to_new(tenant)
        return reset if tenant.nil?

        Apartment.connection.execute "ALTER SESSION SET CURRENT_SCHEMA = #{environmentify(tenant)}"

      rescue ActiveRecord::StatementInvalid => exception
        Apartment::Tenant.reset
        raise_connect_error!(tenant, exception)
      end

      def process_excluded_model(model)
        model.constantize.tap do |klass|
          # Ensure that if a schema *was* set, we override
          table_name = klass.table_name.split('.', 2).last

          klass.table_name = "#{default_tenant}.#{table_name}"
        end
      end

      def reset_on_connection_exception?
        true
      end
    end
  end
end
