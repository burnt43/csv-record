require 'hashie'
require 'active_support'
require 'active_support/core_ext/object/blank'

module CsvRecord
  class Base
    attr_reader :attribute_mash

    class << self
      # config methods
      def csv_filename=(value)
        @csv_filename = value
      end
      def csv_filename
        @csv_filename
      end

      def first_line_contains_schema_info=(value)
        @first_line_contains_schema_info = value
      end
      def first_line_contains_schema_info?
        @first_line_contains_schema_info.nil? ? true : @first_line_contains_schema_info
      end

      def schema_type=(value)
        @schema_type = value
      end
      def schema_type
        @schema_type || :names
      end

      def data_type=(value)
        @data_type = value
      end
      def data_type
        @data_type || :values
      end

      def add_index(attribute_name, options={})
        (@index_options ||= {})[attribute_name.to_sym] = options
      end
      def index_options
        @index_options || {}
      end

      # association methods

      # TODO: guess missing options based on convention
      def belongs_to(attribute_name, options={})
        reflection = CsvRecord::Reflection::BelongsToReflection.new(attribute_name.to_sym, options)
        store_reflection(reflection)

        define_method attribute_name do
          reflection = self.class.reflect_on_association(attribute_name)

          reflection.klass.find_by(reflection.association_primary_key => send(reflection.foreign_key))
        end
      end

      def has_many(attribute_name, options={})
        reflection = CsvRecord::Reflection::HasManyReflection.new(attribute_name.to_sym, options)
        store_reflection(reflection)

        define_method attribute_name do
          reflection = self.class.reflect_on_association(attribute_name)

          reflection.klass.find_all_by(reflection.foreign_key => send(reflection.association_primary_key))
        end
      end

      def store_reflection(reflection)
        (@stored_reflections ||= {})[reflection.name] = reflection
      end
      def stored_reflections
        @stored_reflections || {}
      end
      
      def reflect_on_association(name)
        stored_reflections[name]
      end

      # finder methods
      def find_all_by(attribute_values={})
        []
      end

      def find_by(attribute_values={})
        if attribute_values.size == 0
          # nothing is given, so return nothing
          nil
        elsif attribute_values.size == 1
          # 1 key/value pair was given, so try to see if there is
          # an index on the key and use the index if possible, otherwise
          # do a slow linear search.
          attribute_name = attribute_values.keys.first
          attribute_value = attribute_values.values.first

          if index_options.key?(attribute_name)
            all(indexed_by: attribute_name)[attribute_value]
          else
            all.find {|object| object.send(attribute_name) == attribute_value}
          end
        else
          # multiple key/values were given. instead of doing some fancy
          # stuff with multiple potential indexes, just do a slow linear search.
          all.find do |object|
            attribute_values.all do |attribute_name, attribute_value|
              object.send(attribute_name) == attribute_value
            end
          end
        end
      end

      def all(indexed_by: nil)
        if indexed_by
          if csv_data[:indexed_by].key?(indexed_by)
            csv_data[:indexed_by][indexed_by]
          else
            raise ArgumentError.new("#{self.name} is not indexed_by '#{indexed_by}'")
          end
        else
          csv_data[:unindexed]
        end
      end

      # schema methods
      def attribute_names
        all.first.attribute_mash.keys
      end

      # helper methods
      def safe_data_name(input)
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

      # csv methods
      def csv_data
        @csv_data ||= (
          result            = {unindexed:  [], indexed_by: {}}
          schema            = []
          raw_file_string   = IO.read(csv_filename).force_encoding('iso-8859-1')
          line_number       = 1
          parse_state       = :outside_quote
          csv_record_buffer = []
          value_buffer      = String.new

          value_found_proc = Proc.new {
            value_buffer.blank? ? csv_record_buffer.push(nil) : csv_record_buffer.push(value_buffer.clone)
            value_buffer.clear
          }
          csv_record_found_proc = Proc.new {
            #puts "\033[0;32m#{line_number}\033[0;0m #{csv_record_buffer.to_s}"

            unless csv_record_buffer.compact.empty?
              if line_number == 1 && first_line_contains_schema_info?
                case schema_type
                when :names
                  schema = csv_record_buffer.map {|attribute_name| safe_data_name(attribute_name)}
                end
              else
                attribute_mash = Hashie::Mash.new

                case data_type
                when :name_value_pairs
                  csv_record_buffer.each_slice(2).each do |name, value|
                    attribute_mash[safe_data_name(name)] = value
                  end
                when :values
                  schema.zip(csv_record_buffer).each do |name, value|
                    attribute_mash[safe_data_name(name)] = value
                  end
                end

                object = new(attribute_mash)

                result[:unindexed].push(object)

                index_options.each {|attribute_name, options|
                  if options[:has_many]
                    ((result[:indexed_by][attribute_name] ||= {})[object.send(attribute_name)] ||= []).push(object)
                  else
                    (result[:indexed_by][attribute_name] ||= {})[object.send(attribute_name)] = object
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
    end

    # instance methods
    def initialize(attribute_mash)
      @attribute_mash = attribute_mash
    end

    def method_missing(method_name, *args, &block)
      # delegate to the mash object
      @attribute_mash.send(method_name, *args, &block)
    end
  end # Base

  module Reflection
    class AssociationReflection
      attr_reader :name

      def initialize(name, association_options={})
        @name = name
        @association_options = association_options
      end

      def klass
        @klass ||= Object.const_get(@association_options[:class_name])
      end

      def foreign_key
        @association_options[:foreign_key]
      end

      def association_primary_key
        @association_options[:association_primary_key]
      end
    end

    BelongsToReflection = Class.new(AssociationReflection)
    HasManyReflection   = Class.new(AssociationReflection)
  end # Reflection
end # CsvRecord
