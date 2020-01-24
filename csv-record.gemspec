Gem::Specification.new do |s|
  s.name        = 'csv-record'
  s.version     = '0.0.6'
  s.summary     = 'active_record-like, but for csv files'
  s.description = 'active_record-like, but for csv files'
  s.authors     = ['James Carson']
  s.email       = 'jms.crsn@gmail.com'
  s.homepage    = "http://tmpurl.com"
  s.files       = ['lib/csv-record.rb']
  s.license     = 'MIT'
  s.add_runtime_dependency 'activesupport'
  s.add_runtime_dependency 'hashie'
end
