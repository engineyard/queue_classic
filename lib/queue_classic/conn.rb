module QC
  class Connections
    attr_reader :connections, :connections_mutex

    def initialize
      @connections = Array.new
      @connections_mutex = Mutex.new
    end

    def get_connection
      acquired_connection = nil
      connections_mutex.synchronize do
        found = false
        # Ensure each thread has an id
        if Thread.current[:thread_id].nil?
          Thread.current[:thread_id] = UUIDTools::UUID.random_create.to_str
        end
        # If this thread already has an open connection, get it
        open_connections = connections.select {|c|
          c.thread_id == Thread.current[:thread_id]}
        if open_connections.length > 0
          acquired_connection = open_connections.at(0)
          acquired_connection.mutex.lock unless acquired_connection.mutex.locked?
        else
          # If no open connection exists for this thread, find or create one
          connections.each do |connection|
            if (!found && connection.mutex.try_lock)
              acquired_connection = connection
              found = true
            end
          end
          if acquired_connection.nil?
            connections << QC::Conn.connect
            connections.last.define_singleton_method(:mutex) do
              unless @mutex
                @mutex = Mutex.new
              end
              @mutex
            end
            connections.last.define_singleton_method(:thread_id) do
              unless @thread_id
                @thread_id = nil
              end
              @thread_id
            end
            connections.last.define_singleton_method(:thread_id=) do |val|
              @thread_id = val
            end
            acquired_connection = connections.last
            acquired_connection.mutex.lock
          end
          acquired_connection.thread_id = Thread.current[:thread_id]
        end
      end
      acquired_connection
    end

    def cleanup
      connections_mutex.synchronize do
        connections.select {|c|
          c.thread_id == Thread.current[:thread_id]}.each do |conn|
            conn.mutex.unlock if conn.mutex.locked?
            conn.close
            connections.delete(conn)
        end
      end
    end
    #def get_connection
    #  acquired_connection = nil
    #  self.synchronize do
    #    if self[Thread.current[:qc_conn_id]].nil?
    #      Thread.current[:qc_conn_id] = UUIDTools::UUID.random_create.to_str
    #      self[Thread.current[:qc_conn_id]] = QC::Conn.connect
    #      puts "#{self.length} QUEUE CLASSIC CONNECTIONS FOR PID #{Process.pid}"
    #    end
    #    acquired_connection = self[Thread.current[:qc_conn_id]]
    #  end
    #  return acquired_connection
    #end
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
      rescue PGError => e
        log(:error => e.inspect)
        disconnect
        raise
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
      connection.mutex.unlock
    end

    def cleanup
      connections.cleanup
    end

    def disconnect
      connection.finish
      connection.mutex.unlock
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
