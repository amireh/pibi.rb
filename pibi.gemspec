require File.join(%W[#{File.dirname(__FILE__)} lib pibi version])

Gem::Specification.new do |s|
  s.name        = 'pibi'
  s.summary     = 'Pibi shared Ruby modules.'
  s.version     = Pibi::VERSION
  s.date        = Time.now.strftime('%Y-%m-%d')
  s.authors     = ["Ahmad Amireh"]
  s.email       = 'ahmad@algollabs.com'
  s.homepage    = 'https://github.com/amireh/pibi.rb'
  s.files       = Dir.glob("{lib,spec}/**/*.rb") +
                  [ 'LICENSE', 'README.md', '.rspec', '.yardopts', __FILE__ ]
  s.has_rdoc    = 'yard'
  s.license     = 'MIT'

  s.required_ruby_version = '>= 1.9.3'

  s.add_dependency 'uuid'
  s.add_dependency 'amqp', '~> 0.9.0'
  s.add_dependency 'json'
  s.add_dependency 'activesupport', '>= 4.0.0'

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'yard', '>= 0.8.0'
end
