module PlainView
  module ConnectionAdapters
    module SchemaStatements
      def self.included(base)
        base.alias_method_chain :drop_table, :cascade
      end
    end
    
    module Mysql2Adapter
      def self.included(base)
        if base.private_method_defined?(:supports_views?)
          base.send(:public, :supports_views?)
        end
      end

      # Returns true as this adapter supports views.
      def supports_views?
        true
      end
      
      def base_tables(name = nil) #:nodoc:
        execute("SHOW FULL TABLES WHERE TABLE_TYPE='BASE TABLE'").collect{|row| row[0]}
      end

      def tables_with_views_included(name = nil)
	  	execute("SHOW FULL TABLES").collect {|row| row[0]}
      end
      
      alias nonview_tables base_tables
      
      def views(name = nil) #:nodoc:
        views = []
        execute("SHOW FULL TABLES WHERE TABLE_TYPE='VIEW'").each{|row| views << row[0]}
        views
      end
      
      def structure_dump
        structure = ""
        base_tables.each do |table|
          structure += select_one("SHOW CREATE TABLE #{quote_table_name(table)}")["Create Table"] + ";\n\n"
        end

        views.each do |view|
          structure += select_one("SHOW CREATE VIEW #{quote_table_name(view)}")["Create View"] + ";\n\n"
        end

        return structure
      end

      # Get the view select statement for the specified table.
      def view_select_statement(view, name=nil)
        begin
          execute("SET SESSION sql_mode='ANSI'")
          row = execute("SHOW CREATE VIEW #{view}", name).each do |row|
            return row[1].gsub(/"/, "`") #convert_statement(row[1]) if row[0] == view
          end
        rescue ActiveRecord::StatementInvalid => e
          raise "No view called #{view} found"
        end
      end
      
      private
      def convert_statement(s)
        s.gsub!(/.* AS (select .*)/, '\1')
      end
    end
  end
end
