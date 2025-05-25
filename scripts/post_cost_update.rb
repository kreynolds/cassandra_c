#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "net/http"
require "uri"
require "openssl"
require "base64"
require "optparse"

# Script to post CassandraC AI development cost updates to X (Twitter)
#
# Usage:
#   ruby scripts/post_cost_update.rb [feature_name_or_id]       # Post to X
#   ruby scripts/post_cost_update.rb --dry-run [feature_name_or_id]   # Preview only
#   ruby scripts/post_cost_update.rb --validate                 # Test credentials
#   ruby scripts/post_cost_update.rb --list                     # List available features
#
# Environment variables required for posting (set in .env file):
# - X_API_KEY: Your X API key
# - X_API_SECRET: Your X API secret
# - X_ACCESS_TOKEN: Your X access token
# - X_ACCESS_TOKEN_SECRET: Your X access token secret

class XCostPoster
  BASE_URL = "https://api.twitter.com/2/tweets"
  VERIFY_URL = "https://api.twitter.com/2/users/me"

  def initialize(dry_run: false, debug: false)
    @dry_run = dry_run
    @debug = debug

    unless @dry_run
      load_env_file

      @api_key = ENV["X_API_KEY"]
      @api_secret = ENV["X_API_SECRET"]
      @access_token = ENV["X_ACCESS_TOKEN"]
      @access_token_secret = ENV["X_ACCESS_TOKEN_SECRET"]

      validate_credentials!
    end
  end

  def validate_auth
    puts "🔐 Validating X API credentials..."

    response = make_request("GET", VERIFY_URL)

    if response.code == "200"
      data = JSON.parse(response.body)
      username = data.dig("data", "username")
      puts "✅ Credentials valid! Authenticated as @#{username}"
      true
    else
      puts "❌ Credential validation failed"
      puts "Status: #{response.code}"
      puts "Response: #{response.body}" if @debug
      false
    end
  rescue => e
    puts "❌ Error validating credentials: #{e.message}"
    puts e.backtrace if @debug
    false
  end

  def list_features
    costs_data = parse_costs_file

    puts "📋 Available features in COSTS.md:"
    puts "=" * 50

    if costs_data[:features].empty?
      puts "No features found with cost information"
      return
    end

    # Show features in descending order (newest first) but keep original indexing
    feature_array = costs_data[:features].to_a
    feature_array.reverse.each_with_index do |(name, data), reverse_index|
      # Calculate original index for ID consistency
      original_index = feature_array.length - reverse_index
      status = (name == costs_data[:latest_feature]) ? " (latest)" : ""
      duration_info = data[:duration] ? " - #{data[:duration]}" : ""
      puts "#{original_index}. #{name}#{status}"
      puts "   Cost: $#{data[:cost]}#{duration_info}"

      # Show first few features
      if data[:features] && !data[:features].empty?
        first_features = data[:features].first(2)
        first_features.each { |feature| puts "   #{feature}" }
        puts "   ..." if data[:features].length > 2
      end
      puts
    end

    puts "📊 Totals:"
    puts "   Total cost: $#{costs_data[:totals][:total_cost] || "N/A"}"
    puts "   Total features: #{costs_data[:totals][:total_features] || "N/A"}"
    puts
    puts "💡 Use feature name or ID as argument to post about specific feature"
    puts "   Example: ruby #{$0} \"Counter Types Support\""
    puts "   Example: ruby #{$0} 4"
  end

  def post_latest_feature(feature_identifier = nil)
    costs_data = parse_costs_file

    # Determine the feature to post about
    target_feature = if feature_identifier.nil?
      costs_data[:latest_feature]
    elsif feature_identifier.match?(/^\d+$/)
      # Handle numeric ID
      feature_id = feature_identifier.to_i
      feature_names = costs_data[:features].keys
      if feature_id >= 1 && feature_id <= feature_names.length
        feature_names[feature_id - 1]
      else
        puts "❌ Invalid feature ID: #{feature_id}"
        puts "💡 Use --list to see available features (1-#{feature_names.length})"
        exit 1
      end
    else
      # Handle feature name
      feature_identifier
    end

    if target_feature.nil?
      puts "❌ No feature specified and couldn't determine latest feature"
      puts "💡 Use --list to see available features"
      exit 1
    end

    feature_data = costs_data[:features][target_feature]
    if feature_data.nil?
      puts "❌ Feature '#{target_feature}' not found in COSTS.md"
      puts "💡 Use --list to see available features"
      exit 1
    end

    puts "🎯 Posting about: #{target_feature}"

    # Generate tweets
    main_tweet = generate_main_tweet(target_feature, feature_data, costs_data[:totals])
    reply_tweet = generate_reply_tweet(target_feature, feature_data, costs_data[:totals])

    # Show preview
    puts "📝 Main tweet (#{main_tweet.length} chars):"
    puts "=" * 50
    puts main_tweet
    puts "=" * 50

    puts "\n📝 Reply tweet (#{reply_tweet.length} chars):"
    puts "=" * 50
    puts reply_tweet
    puts "=" * 50

    if @dry_run
      puts "\n🔍 DRY RUN - No tweets posted"
      puts "Remove --dry-run to actually post to X"
      return
    end

    puts "\n🚀 Posting main tweet..."

    main_response = post_tweet(main_tweet)

    if main_response.code != "201"
      puts "❌ Failed to post main tweet"
      puts "Status: #{main_response.code}"
      puts "Response: #{main_response.body}"
      exit 1
    end

    main_data = JSON.parse(main_response.body)
    main_tweet_id = main_data.dig("data", "id")
    puts "✅ Main tweet posted! ID: #{main_tweet_id}"

    # Post reply thread with technical details
    puts "\n🚀 Posting reply..."

    reply_response = post_tweet(reply_tweet, reply_to: main_tweet_id)

    if reply_response.code == "201"
      reply_data = JSON.parse(reply_response.body)
      reply_tweet_id = reply_data.dig("data", "id")
      puts "✅ Reply posted! ID: #{reply_tweet_id}"
      puts "🔗 Main tweet: https://twitter.com/i/web/status/#{main_tweet_id}"
      puts "🔗 Thread: https://twitter.com/i/web/status/#{reply_tweet_id}"
    else
      puts "⚠️  Main tweet posted but reply failed"
      puts "🔗 Main tweet: https://twitter.com/i/web/status/#{main_tweet_id}"
      puts "Reply error: #{reply_response.code} - #{reply_response.body}"
    end
  end

  private

  def load_env_file
    env_file = File.join(__dir__, "..", ".env")
    return unless File.exist?(env_file)

    File.readlines(env_file).each do |line|
      line = line.strip
      next if line.empty? || line.start_with?("#")

      key, value = line.split("=", 2)
      next unless key && value

      # Remove quotes if present
      value = value.gsub(/\A['"]|['"]\z/, "")
      ENV[key] = value
    end
  end

  def validate_credentials!
    missing = []
    missing << "X_API_KEY" unless @api_key
    missing << "X_API_SECRET" unless @api_secret
    missing << "X_ACCESS_TOKEN" unless @access_token
    missing << "X_ACCESS_TOKEN_SECRET" unless @access_token_secret

    unless missing.empty?
      puts "❌ Missing required environment variables: #{missing.join(", ")}"
      puts "\nCreate a .env file in the project root with:"
      missing.each { |var| puts "#{var}=your_#{var.downcase}" }
      exit 1
    end
  end

  def parse_costs_file
    costs_file = File.join(__dir__, "..", "COSTS.md")
    unless File.exist?(costs_file)
      puts "❌ COSTS.md not found at #{costs_file}"
      exit 1
    end

    content = File.read(costs_file)
    features = {}
    totals = {}
    latest_feature = nil

    # Parse features - look for cost and duration info
    feature_sections = content.split(/(?=### [^\n]+ Support)/).select { |section| section.include?("**Cost**") }

    feature_sections.each do |section|
      if section =~ /### ([^\n]+)\n\*\*Cost\*\*: \$([0-9.]+)/
        clean_name = $1.strip
        cost = $2.to_f
        latest_feature = clean_name # Last one parsed will be latest

        # Extract duration if present
        duration = nil
        if section =~ /\*\*Duration\*\*: ([^\n]+)/
          duration = $1.strip
        end

        # Extract features
        features_text = ""
        if section =~ /\*\*Features Implemented\*\*:\n(.*?)(?=\n\*\*Key Deliverables|### |## |\z)/m
          features_text = $1.strip
        end

        features[clean_name] = {
          cost: cost,
          duration: duration,
          features: features_text.split("\n").map(&:strip).reject(&:empty?)
        }
      end
    end

    # Parse totals
    if content =~ /- \*\*Total Cost\*\*: \$([0-9.]+)/
      totals[:total_cost] = $1.to_f
    end

    if content =~ /- \*\*Total Features\*\*: ([0-9]+)/
      totals[:total_features] = $1.to_i
    end

    {
      features: features,
      totals: totals,
      latest_feature: latest_feature
    }
  end

  def generate_main_tweet(feature_name, feature_data, totals)
    # High-level engaging tweet optimized for algorithm
    cost = feature_data[:cost]
    duration = feature_data[:duration]

    tweet = "🤖 Claude Code Experiment Update\n\n"
    tweet += "Added production #{feature_name.downcase} to a Ruby gem\n\n"
    tweet += "💰 Cost: $#{sprintf("%.2f", cost)}"

    if duration
      # Extract just the wall time for simplicity
      if duration =~ /(\d+m \d+s).*wall/
        tweet += " | ⏱️ #{$1}"
      end
    end

    tweet += "\n🎯 Result: Production-ready code with tests, docs, and error handling\n\n"

    if totals[:total_cost]
      tweet += "📊 Project total so far: $#{sprintf("%.2f", totals[:total_cost])}"
      if totals[:total_features]
        tweet += " (#{totals[:total_features]} features)"
      end
      tweet += "\n\n"
    end

    tweet += "Thread with technical details ↓"

    tweet
  end

  def generate_reply_tweet(feature_name, feature_data, totals)
    # Technical details for developers in the thread
    feature_data[:cost]
    main_features = feature_data[:features].first(3) # Take first 3 features

    tweet = "🔧 Technical details:\n\n"

    main_features.each do |feature|
      clean_feature = feature.gsub(/^[-•]\s*/, "").strip
      # Truncate long features
      clean_feature = clean_feature[0..60] + "..." if clean_feature.length > 63
      tweet += "• #{clean_feature}\n"
    end

    tweet += "\n✅ All tests passing, linted, documented"
    tweet += "\n📦 Native C extension + Ruby wrapper"

    # Try to find specific commit for this feature
    commit_hash = find_feature_commit(feature_name)
    tweet += if commit_hash
      "\n🔗 https://github.com/kreynolds/cassandra_c/commit/#{commit_hash}"
    else
      "\n🔗 https://github.com/kreynolds/cassandra_c"
    end

    tweet
  end

  def post_tweet(text, reply_to: nil)
    body = {text: text}
    body[:reply] = {in_reply_to_tweet_id: reply_to} if reply_to

    make_request("POST", BASE_URL, body)
  end

  def make_request(method, url, body = nil)
    uri = URI(url)

    # Create the request
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    request = case method
    when "GET"
      Net::HTTP::Get.new(uri)
    when "POST"
      Net::HTTP::Post.new(uri)
    end

    request["Content-Type"] = "application/json" if body
    request["Authorization"] = generate_oauth_header(method, url, body || {})
    request.body = JSON.generate(body) if body

    puts "Making #{method} request to #{url}" if @debug
    puts "Request body: #{request.body}" if @debug && body

    response = http.request(request)
    puts "Response: #{response.code} - #{response.body}" if @debug
    response
  end

  def generate_oauth_header(method, url, params = {})
    # OAuth 1.0a signature generation for X API v2
    oauth_params = {
      "oauth_consumer_key" => @api_key,
      "oauth_token" => @access_token,
      "oauth_signature_method" => "HMAC-SHA1",
      "oauth_timestamp" => Time.now.to_i.to_s,
      "oauth_nonce" => SecureRandom.hex(16),
      "oauth_version" => "1.0"
    }

    # Create signature base string
    all_params = oauth_params.merge(params.transform_keys(&:to_s))
    param_string = all_params.sort.map { |k, v| "#{encode(k)}=#{encode(v)}" }.join("&")
    base_string = "#{method}&#{encode(url)}&#{encode(param_string)}"

    # Create signing key
    signing_key = "#{encode(@api_secret)}&#{encode(@access_token_secret)}"

    # Generate signature
    signature = Base64.strict_encode64(
      OpenSSL::HMAC.digest("sha1", signing_key, base_string)
    )

    oauth_params["oauth_signature"] = signature

    # Build authorization header
    header_params = oauth_params.sort.map { |k, v| "#{k}=\"#{encode(v)}\"" }.join(", ")
    "OAuth #{header_params}"
  end

  def encode(value)
    URI.encode_www_form_component(value.to_s)
  end

  def find_feature_commit(feature_name)
    # Search recent commits for one that matches the feature
    search_terms = feature_name.downcase.split(/\s+/)

    # Get recent commit log
    result = `git log --oneline -20 2>/dev/null`
    return nil unless $?.success?

    result.lines.each do |line|
      commit_hash, message = line.strip.split(" ", 2)
      next unless message

      message_lower = message.downcase

      # Check if commit message contains key terms from feature name
      if search_terms.any? { |term| message_lower.include?(term) }
        return commit_hash
      end
    end

    nil
  rescue
    nil
  end
end

# Main execution
if __FILE__ == $0
  options = {
    dry_run: false,
    debug: false,
    validate: false,
    list: false
  }

  OptionParser.new do |opts|
    opts.banner = "Usage: #{$0} [options] [feature_name_or_id]"

    opts.on("-n", "--dry-run", "Preview tweets without posting") do
      options[:dry_run] = true
    end

    opts.on("-d", "--debug", "Enable debug output") do
      options[:debug] = true
    end

    opts.on("-v", "--validate", "Validate API credentials") do
      options[:validate] = true
    end

    opts.on("-l", "--list", "List available features") do
      options[:list] = true
    end

    opts.on("-h", "--help", "Show this help") do
      puts opts
      exit
    end
  end.parse!

  feature_name = ARGV.first

  begin
    poster = XCostPoster.new(dry_run: options[:dry_run], debug: options[:debug])

    if options[:validate]
      exit poster.validate_auth ? 0 : 1
    elsif options[:list]
      poster.list_features
    else
      poster.post_latest_feature(feature_name)
    end
  rescue => e
    puts "❌ Error: #{e.message}"
    puts e.backtrace if options[:debug]
    exit 1
  end
end
