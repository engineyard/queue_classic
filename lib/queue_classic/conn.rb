module QC
  class Connections
    attr_reader :connections, :connections_mutex

    def initialize
      @connections = Hash.new
      @connections_mutex = Mutex.new
    end

    def get_connection
      # If the connection has already been created for this thread and is open,
      # just return it without locking.
      unless @connections[Thread.current] &&
        @connections[Thread.current].finished? == false
          # If we're at this point, we have not yet secured an open connection.  Open
          # a new one.
          new_connection = QC::Conn.connect

          @connections_mutex.synchronize do
            # Take this opportunity to clean up any connections associated with dead
            # threads.
            @connections.keys.each do |connection_thread|
              if connection_thread.status.nil? ||
                 connection_thread.status == false
                @connections[connection_thread].finish if @connections[connection_thread].finished? == false
                @connections.delete(connection_thread)
              end
            end
            @connections[Thread.current] = new_connection
          end
      end
      # Return the connection that is specific for the thread
      @connections[Thread.current]
    end

    def add_connection_instance_property connection_instance, property_name, aliases = []
      unless connection_instance.respond_to?(property_name) ||
             aliases.select { |a| connection_instance.respond_to?(a)}.length > 0
        property_code =  "def #{property_name}; "
        property_code += "@#{property_name}; end; "
        property_code += "def #{property_name}=(value); "
        property_code += "@#{property_name}=value; end; "
        connection_instance.class.class_eval(property_code)

        aliases.each do |property_alias|
          connection_instance.class.class_eval("alias :#{property_alias} :#{property_name}")
        end
      end
    end

  end

  module Conn

    extend self

    def connections
      unless @connections
        @connections = QC::Connections.new
      end
      @connections
    end

    def execute(stmt, *params)
      log(:level => :debug, :action => "exec_sql", :sql => stmt.inspect)
      begin
        params = nil if params.empty?
        r = connection.exec(stmt, params)
        result = []
        r.each {|t| result << t}
        result.length > 1 ? result : result.pop
      rescue => exception
        if exception.message.include?("no connection to the server")
          # Attempt to reconnect ONCE.  If that doesn't work, fail.
          connection.close if is_open?
          begin
            connection = connect
          rescue => reconnect_exception
            log(:error => e.inspect)
            connection.close
            raise "Postgres Database connection lost.  Reconnect failed: #{reconnect_exception.message}\n#{reconnect_exception.backtrace}"
          end
        else
          raise exception
        end
      end
    end

    def notify(chan)
      log(:level => :debug, :action => "NOTIFY")
      execute('NOTIFY "' + chan + '"') #quotes matter
    end

    def listen(chan)
      log(:level => :debug, :action => "LISTEN")
      execute('LISTEN "' + chan + '"') #quotes matter
    end

    def unlisten(chan)
      log(:level => :debug, :action => "UNLISTEN")
      execute('UNLISTEN "' + chan + '"') #quotes matter
    end

    def drain_notify
      until connection.notifies.nil?
        log(:level => :debug, :action => "drain_notifications")
      end
    end

    def wait_for_notify(t)
      connection.wait_for_notify(t) do |event, pid, msg|
        log(:level => :debug, :action => "received_notification")
      end
    end

    def transaction
      begin
        execute("BEGIN")
        yield
        execute("COMMIT")
      rescue Exception
        execute("ROLLBACK")
        raise
      end
    end

    def transaction_idle?
      connection.transaction_status == PGconn::PQTRANS_IDLE
    end

    def connection
      connections.get_connection
      #@connection ||= connect
    end

    def finish
      connection.finish
    end

    def cleanup
      #connections.cleanup
    end

    def is_open?
      !connection.finished?
    end

    def disconnect
      connection.finish
    end

    def connect
      log(:level => :debug, :action => "establish_conn")
      conn = PGconn.connect(
        db_url.host,
        db_url.port || 5432,
        nil, '', #opts, tty
        db_url.path.gsub("/",""), # database name
        db_url.user,
        db_url.password
      )
      schema  = db_url.query.to_s.split('&').detect { |k| k.match /schema=/ }.to_s.sub(/.*=/,'')

      if conn.status != PGconn::CONNECTION_OK
        log(:level => :error, :message => conn.error)
      end

      conn.exec("SET search_path TO #{schema}") unless schema.nil? || schema == ""

      conn
    end

    def db_url
      return @db_url if @db_url
      url = ENV["QC_DATABASE_URL"] ||
            ENV["DATABASE_URL"]    ||
            raise(ArgumentError, "missing QC_DATABASE_URL or DATABASE_URL")
      @db_url = URI.parse(url)
    end

    def log(msg)
      QC.log(msg)
    end

  end
end
