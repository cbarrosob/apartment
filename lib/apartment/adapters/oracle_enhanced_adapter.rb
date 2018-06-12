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

      delegate :connection, :establish_connection, to: ActiveRecord::Base


      def initialize(config)
        super

        @default_tenant = config[:username]
      end

    def create(connection, tenant)
      begin
        connection.execute "CREATE USER #{tenant} IDENTIFIED BY #{tenant}"
      rescue => e
        if e.message =~ /ORA-01920/ # user name conflicts with another user or role name
          connection.execute "ALTER USER #{tenant} IDENTIFIED BY #{tenant}"
        else
          raise e
        end
      end

      connection.execute "GRANT unlimited tablespace TO #{tenant}"
      connection.execute "GRANT create session TO #{tenant}"
      connection.execute "GRANT create table TO #{tenant}"
      connection.execute "GRANT create view TO #{tenant}"
      connection.execute "GRANT create sequence TO #{tenant}"

      grant_select_on_principal_excluded_models(connection, tenant)
    end

    private

      def grant_select_on_principal_excluded_models(connection, tenant)
        Apartment.excluded_models.each do |model|
          if ActiveRecord::Base.connection.tables.include? model.downcase.pluralize
            connection.execute "GRANT SELECT on #{@config[:username]}.#{model.downcase.pluralize} to #{tenant}"
          end
        end
      end

      def create_tenant_command(conn, tenant)
        conn.create(environmentify(tenant), @config)
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
