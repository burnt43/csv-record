require 'hashie'
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/string/inflections'

module Kernel
  def jcarson_debug(msg)
    puts "[JCARSON] - \033[0;32m#{msg}\033[0;0m"
  end
end

module CsvRecord
  class << self
    def namespace=(value)
      @namespace_string = value
    end
    def namespace
      @namespace ||= @namespace_string.nil? ? Object : Object.const_get(@namespace_string)
    end
  end

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

      def modify_attribute_name(new_name, old_name)
        (@modify_attribute_name_config ||= {})[old_name.to_sym] = new_name.to_sym
      end
      def modify_attribute_name_config
        @modify_attribute_name_config || {}
      end

      attr_accessor :primary_key

      # association methods

      # TODO: guess missing options based on convention
      def belongs_to(attribute_name, options={})
        reflection = CsvRecord::Reflection::BelongsToReflection.new(
          self,
          attribute_name.to_sym,
          options
        )
        store_reflection(reflection)

        define_method attribute_name do
          reflection = self.class.reflect_on_association(attribute_name)

          reflection.klass.find_by(reflection.association_primary_key => send(reflection.foreign_key))
        end
      end

      def has_many(attribute_name, options={})
        reflection_type =
          if options.key?(:through)
            CsvRecord::Reflection::ThroughReflection
          else
            CsvRecord::Reflection::HasManyReflection
          end

        reflection = reflection_type.new(
          self,
          attribute_name.to_sym,
          options
        )
        store_reflection(reflection)

        define_method attribute_name do
          reflection = self.class.reflect_on_association(attribute_name)

          if reflection.is_a?(CsvRecord::Reflection::HasManyReflection)
            reflection.klass.find_all_by(
              reflection.foreign_key => send(reflection.association_primary_key)
            )
          elsif reflection.is_a?(CsvRecord::Reflection::ThroughReflection)
            reflection.through_reflection.klass.find_all_by(
              reflection.through_reflection.foreign_key => send(reflection.through_reflection.association_primary_key)
            ).map do |record|
              record.send(reflection.source_reflection.name)
            end
          end
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
        master_find_by(false, attribute_values)
      end

      def find_by(attribute_values={})
        master_find_by(true, attribute_values)
      end

      def find(primary_key_value)
        if primary_key
          find_by(primary_key => primary_key_value)
        else
          # FIXME: ArgumentError is not really appropriate here.
          raise ArgumentError.new("#{self.name} has no primary_key configured.")
        end
      end

      def master_find_by(singular=true, attribute_values={})
        if attribute_values.size == 0
          # nothing is given, so return nothing
          singular ? nil : []
        elsif attribute_values.size == 1
          # 1 key/value pair was given, so try to see if there is
          # an index on the key and use the index if possible, otherwise
          # do a slow linear search.
          attribute_name = attribute_values.keys.first
          attribute_value = attribute_values.values.first

          if index_options.key?(attribute_name)
            result = all(indexed_by: attribute_name)[attribute_value]
            singular ? result : Array(result)
          else
            missing_index_warning(attribute_name)
            method_name = singular ? :find : :find_all

            all.send(method_name) {|object| object.send(attribute_name) == attribute_value}
          end
        else
          # multiple key/values were given. instead of doing some fancy
          # stuff with multiple potential indexes, just do a slow linear search.
          method_name = singular ? :find : :find_all

          all.send(method_name) do |object|
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
            raise ArgumentError.new("#{self.name} is not indexed_by '#{indexed_by}'.")
          end
        else
          csv_data[:unindexed]
        end
      end

      # schema methods
      def attribute_names
        load_csv! unless csv_loaded?
        @attribute_names
      end

      def potential_foreign_key_attribute_names
        attribute_names.select {|name| name.end_with?('id')}
      end

      def missing_index_warning(attribute_name)
        STDERR.puts("[WARNING] - missing index '#{attribute_name}'")
      end

      # helper methods
      def schemaize_attribute_name(input)
        # REVIEW: this conversion is some stuff that was necessary for 1 certain
        # project, but maybe it should not exist at all and be configurable by the
        # users of this library.

        result =
          input
          .underscore
          .gsub(%r/\s+/, '_')      # white space becomes a single underscore
          .gsub(%r/[\.\+\|]/, '')  # characters to remove
          .gsub(%r/\//, '_')       # / becomes _
          .gsub(%r/#/, '_number')  # # becomes _number
          .sub(%r/_\z/, '')        # trailing underscores are removed for Hashie compatibility

        if modify_attribute_name_config.key?(result.to_sym)
          modify_attribute_name_config[result.to_sym].to_s
        else
          result
        end
      end

      def stored_attribute_name(input)
        reserved_ruby_method_conversion =
          case input
          when 'class' then 'klass'
          else input
          end

        "__#{reserved_ruby_method_conversion}"
      end

      # csv methods
      def csv_loaded?
        !@csv_data.nil?
      end

      def load_csv!
        if csv_loaded?
          false
        else
          csv_data
          true
        end
      end

      def csv_data
        @csv_data ||= (
          result            = {unindexed:  [], indexed_by: {}}
          @attribute_names  = []
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
                  @attribute_names = csv_record_buffer.map {|attribute_name| schemaize_attribute_name(attribute_name)}
                end
              else
                attribute_mash = Hashie::Mash.new

                case data_type
                when :name_value_pairs
                  csv_record_buffer.each_slice(2).each do |name, value|
                    attribute_name = stored_attribute_name(schemaize_attribute_name(name))
                    @attribute_names.push(attribute_name)
                    attribute_mash[attribute_name] = value&.strip
                  end
                when :values
                  @attribute_names.zip(csv_record_buffer).each do |name, value|
                    attribute_name = stored_attribute_name(name)
                    attribute_mash[attribute_name] = value&.strip
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

          result
        )
      end
    end

    # instance methods
    def initialize(attribute_mash)
      @attribute_mash = attribute_mash
    end

    def attributes
      @attribute_mash.to_hash
    end

    def print_attributes(
      only: [],
      except: []
    )
      collection =
        if !only.empty?
          only_as_stored = only.map {|attr| self.class.stored_attribute_name(attr)}
          attributes.slice(*only_as_stored)
        elsif !except.empty?
          except_as_stored = except.map {|attr| self.class.stored_attribute_name(attr)}
          attributes.except(*except_as_stored)
        else
          attributes
        end

      collection.each do |attribute_name, attribute_value|
        printf("%-40s %-40s\n", attribute_name, attribute_value)
      end
    end

    def method_missing(method_name, *args, &block)
      if method_name.to_s.end_with?('=')
        super
      else
        stored_attribute_name = self.class.stored_attribute_name(method_name)

        if @attribute_mash.respond_to?(stored_attribute_name)
          # Assume this method is a lookup of an attribute and
          # delegate to the mash object.
          @attribute_mash.send(self.class.stored_attribute_name(method_name), *args, &block)
        else
          super
        end
      end
    end
  end # Base

  module Reflection
    class AssociationReflection
      attr_reader :name

      def initialize(csv_record, name, options={})
        @csv_record = csv_record
        @name = name
        @options = options
      end

      def class_name
        @options[:class_name] || @name.to_s.classify
      end

      def klass
        @klass ||= CsvRecord.namespace.const_get(class_name)
      end

      def foreign_key
        @options[:foreign_key]
      end

      def association_primary_key
        @options[:association_primary_key]
      end
    end

    class BelongsToReflection < AssociationReflection
      def association_primary_key
        @options[:association_primary_key] || @klass.primary_key
      end
    end

    class HasManyReflection < AssociationReflection
      def association_primary_key
        @options[:association_primary_key] || @csv_record.primary_key
      end
    end

    class ThroughReflection < AssociationReflection
      def through_association_name
        @options[:through]
      end

      def source_association_name
        @options[:source]
      end

      def through_reflection
        @through_reflection ||= @csv_record.reflect_on_association(through_association_name)
      end

      def source_reflection
        @source_reflection ||= through_reflection.klass.reflect_on_association(source_association_name)
      end
    end
  end # Reflection
end # CsvRecord
