module QC
  class Connections
    attr_reader :connections, :connections_mutex

    def initialize
      @connections = Array.new
      @connections_mutex = Mutex.new
    end

    def get_connection
      # If the connection has already been created for this thread,
      # just return it without locking.
      acquired_connection = nil
      found_connections = connections.select {|c|
        c[:thread_id] == Thread.current[:thread_id] &&
        c[:thread].nil? == false &&
        c[:thread].status.nil? == false &&
        c[:thread].status != false}

      if found_connections.length > 0
        acquired_connection = found_connections.at(0)[:connection]
      else
        # If we're at this point, we have not yet secured an open connection.  Open
        # one.

        # Ensure each thread has an id
        if Thread.current[:thread_id].nil?
          Thread.current[:thread_id] = UUIDTools::UUID.random_create.to_str
        end
        # Snag or allocate the connection in the connections array
        connections_mutex.synchronize do
          i = @connections.rindex {|conn| conn[:thread].nil? || conn[:thread].status.nil? ||
                                          conn[:thread].status == false}
          if i.nil?
            new_connection = {:thread_id => Thread.current[:thread_id],
                              :thread => Thread.current,
                              :connection => QC::Conn.connect}
            connections << new_connection
            acquired_connection = new_connection[:connection]
          else
            # If the thread in this spot in the array is finished with this connection,
            # give it to another thread.
            #connections[i][:connection].close
            #connections[i] = {:thread_id => Thread.current[:thread_id],
            #                  :thread => Thread.current,
            #                  :connection => QC::Conn.connect}
            connections[i][:thread_id] = Thread.current[:thread_id]
            connections[i][:thread] = Thread.current
            acquired_connection = connections[i][:connection]
          end

        end
      end
      # Return the connection that is specific for the thread
      acquired_connection
    end

    def cleanup
      #connections_mutex.synchronize do
      #  connections.select {|c|
      #    c.thread_id == Thread.current[:thread_id]}.each do |conn|
      #      conn.mutex.unlock if conn.mutex.locked?
      #      conn.close
      #      connections.delete(conn)
      #  end
      #end
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
      #connection.mutex.unlock
    end

    def cleanup
      #connections.cleanup
    end

    def disconnect
      connection.finish
      #connection.mutex.unlock
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
