module CsvRecord
  class Base
    include HasAttributeHash

    attr_reader :attribute_hash

    # config methods
    def self.csv_filename=(value)
      @csv_filename = value
    end
    def self.csv_filename
      @csv_filename
    end

    def self.first_line_contains_schema_info=(value)
      @first_line_contains_schema_info = value
    end
    def self.first_line_contains_schema_info?
      !!@first_line_contains_schema_info
    end

    def self.schema_type=(value)
      @schema_type = value
    end
    def self.schema_type
      @schema_type
    end

    def self.data_type=(value)
      @data_type = value
    end
    def self.data_type
      @data_type
    end

    def self.add_index(attribute_name, options={})
      (@index_options ||= Hash.new)[attribute_name.to_sym] = options
    end
    def self.index_options
      @index_options || Hash.new
    end

    # helper methods
    def self.safe_data_name(input)
      result = ActiveSupport::Inflector.underscore(input)
        .gsub(/\s+/, '_')      # white space becomes a single underscore
        .gsub(/[\.\+]/, '')    # . and + are removed
        .gsub(/\//, '_')       # / becomes _
        .gsub(/#/, '_number')  # # becomes _number

      case result
      when "open"
        "opened"
      when "class"
        "klass"
      else
        result
      end
    end

    # other methods
    def self.csv_data
      @csv_data ||= (
        result            = {unindexed:  Array.new, indexed_by: Hash.new}
        schema            = Array.new
        raw_file_string   = IO.read(self.csv_filename).force_encoding('iso-8859-1')
        line_number       = 1
        parse_state       = :outside_quote
        csv_record_buffer = Array.new
        value_buffer      = String.new

        value_found_proc = Proc.new {
          value_buffer.blank? ? csv_record_buffer.push(nil) : csv_record_buffer.push(value_buffer.clone)
          value_buffer.clear
        }
        csv_record_found_proc = Proc.new {
          #puts "\033[0;32m#{line_number}\033[0;0m #{csv_record_buffer.to_s}"

          unless csv_record_buffer.compact.empty?
            if line_number == 1 && self.first_line_contains_schema_info?
              case self.schema_type
              when :names
                schema = csv_record_buffer.map {|attribute_name| self.safe_data_name(attribute_name)}
              end
            else
              attribute_hash = (case self.data_type
              when :name_value_pairs
                Hash[csv_record_buffer.each_slice(2).map {|name, value|
                  [self.safe_data_name(name), value]
                }]
              when :values
                Hash[schema.zip(csv_record_buffer)]
              end)

              object = self.new(attribute_hash)
              result[:unindexed].push(object)
              self.index_options.each {|attribute_name, options|
                if options[:has_many]
                  ((result[:indexed_by][attribute_name] ||= Hash.new)[object.send(attribute_name)] ||= Array.new).push(object)
                else
                  (result[:indexed_by][attribute_name] ||= Hash.new)[object.send(attribute_name)] = object
                end
              }
            end
          end

          csv_record_buffer.clear
          line_number += 1
        }

        raw_file_string << "\n"
        current_char    = 1
        number_of_chars = raw_file_string.length

        raw_file_string.each_char {|c|
          print "[JCARSON] - (#{self.name}) - analyzing csv char...(#{current_char}/#{number_of_chars})\r"
          case parse_state
          when :outside_quote
            case c
            when '"'
              parse_state = :inside_quote
            when ','
              value_found_proc.call
            when "\n"
              value_found_proc.call
              csv_record_found_proc.call
            when "\r"
              # do nothing
            else
              value_buffer << c
            end
          when :inside_quote
            case c
            when '"'
              parse_state = :quote_found_inside_quote
            when "\r"
              # do nothing
            else
              value_buffer << c
            end
          when :quote_found_inside_quote
            case c
            when ','
              value_found_proc.call
              parse_state = :outside_quote
            when "\n"
              value_found_proc.call
              csv_record_found_proc.call
              parse_state = :outside_quote
            when "\r"
              # do nothing
            when '"'
              value_buffer << '"'
              value_buffer << c
            else
              value_buffer << '"'
              value_buffer << c
              parse_state = :inside_quote
            end
          end
          current_char += 1
        }
        puts ''

        result
      )
    end

    def self.all(options={})
      if options.has_key?(:indexed_by)
        self.csv_data.dig(:indexed_by, options[:indexed_by]) || Hash.new
      else
        self.csv_data[:unindexed]
      end
    end

    def self.attribute_names
      self.all.first.attribute_hash.keys
    end

    # instance methods
    def initialize(attribute_hash)
      @attribute_hash = attribute_hash
    end
  end # Base
end # CsvRecord
