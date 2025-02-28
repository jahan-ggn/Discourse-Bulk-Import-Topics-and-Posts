require 'csv'
require 'time'
require 'ruby-progressbar'
require 'pathname'

# Configuration Constants
LOG_FILE ||= '/Users/apple/Desktop/Bulk Import Script/import_errors.log'
CSV_DIRECTORY ||= '/Users/apple/Desktop/Bulk Import Script/'

def find_latest_csv(directory)
  csv_files = Dir.glob(File.join(directory, "*.csv"))
  return nil if csv_files.empty?

  csv_files.max_by { |file| File.mtime(file) }
end

CSV_FILE = find_latest_csv(CSV_DIRECTORY)


# Script Constants
TAG_SEPARATOR ||= '|'
DEFAULT_CATEGORY_ID ||= 86
DEFAULT_MAIN_POST_USER_EMAIL ||= 'mohasaba.net.01@gmail.com'
DEFAULT_POST_ANSWER_USER_EMAIL ||= 'info@abdelhamidcpa.com'

# Ensure tags exist before assigning to topics
def ensure_tags_exist(tags, row_number)
  return [] if tags.nil? || tags.empty?

  existing_tags = Tag.where(name: tags).pluck(:name)
  new_tags = tags - existing_tags

  new_tags.each do |tag_name|
    tag = Tag.create(name: tag_name)
    if !tag.persisted?
      log.puts "Failed to create tag: #{tag_name} at row #{row_number}"
    end
  end

  existing_tags + new_tags
end

def find_user_by_email(email)
  return nil if email.nil? || email.strip.empty?
  
  User.joins(:user_emails).find_by(user_emails: { email: email.strip })
end

# Import topics from CSV
def import_topics_from_csv(csv_file_path, log_file)
  total_rows = CSV.read(csv_file_path, headers: true, encoding: 'UTF-8').length
  success_count = 0

  progressbar = ProgressBar.create(
    title: "Importing Topics",
    total: total_rows,
    format: "%t |%B| %c/%C Rows Imported (%p%%)"
  )

  File.open(log_file, 'w', encoding: 'UTF-8') do |log|
    log.puts("Import Errors - #{Time.now}\n\n")

    CSV.foreach(csv_file_path, headers: true, encoding: 'UTF-8').with_index(2) do |row, row_number|
      row = row.to_h.transform_keys(&:strip)

      begin
        # Extract and sanitize fields
        title = row['Topic_Title']&.strip
        content = row['Topic_Main_Post']&.strip
        tags = row['Topic_Tags']&.split(TAG_SEPARATOR).map(&:strip).reject(&:empty?) || []
        created_at = Time.now

        # Extract reply details
        post_reply = row['Topic_Post_Answer']&.strip
        post_reply_created_at = Time.now

        # Validate required fields and log row number if skipped
        missing_fields = []
        missing_fields << "Topic_Title" if title.nil? || title.empty?
        missing_fields << "Topic_Main_Post" if content.nil? || content.empty?
        missing_fields << "Topic_Post_Answer" if post_reply.nil? || post_reply.empty?

        # Skip entire row if any field is missing
        if missing_fields.any?
          log.puts("Skipping row #{row_number}: Missing #{missing_fields.join(', ')}")
          next
        end

        # Validate Category
        unless Category.exists?(id: DEFAULT_CATEGORY_ID)
          log.puts("Error: Default Category ID '#{DEFAULT_CATEGORY_ID}' does not exist.")
          next
        end

        # Fetch users
        user = find_user_by_email(DEFAULT_MAIN_POST_USER_EMAIL)
        reply_user = find_user_by_email(DEFAULT_POST_ANSWER_USER_EMAIL)

        if user.nil? || reply_user.nil?
          log.puts("Skipping row #{row_number}: User not found - #{DEFAULT_MAIN_POST_USER_EMAIL} or #{DEFAULT_POST_ANSWER_USER_EMAIL}")
          next
        end

        tags = ensure_tags_exist(tags, row_number)

        topic_options = {
          title: title,
          raw: content,
          category: DEFAULT_CATEGORY_ID,
          tags: tags,
          created_at: created_at,
          import_mode: true,
        }

        topic = TopicCreator.create(user, Guardian.new(user), topic_options)

        if !topic.persisted?
          log.puts("Skipping row #{row_number}: Failed to create topic '#{title}' - Errors: #{topic.errors.full_messages.join(', ')}")
          next
        end

        DiscourseTagging.tag_topic_by_names(topic, user.guardian, tags) if tags.any?

        post = PostCreator.create(user, {
          topic_id: topic.id,
          raw: content,
          created_at: created_at
        })

        if !post.persisted?
          log.puts("Skipping row #{row_number}: Failed to create main post for topic '#{title}' - Errors: #{post.errors.full_messages.join(', ')}")
          topic.destroy
          next
        end

        reply = PostCreator.create(reply_user, {
          topic_id: topic.id,
          raw: post_reply,
          created_at: post_reply_created_at
        })

        if !reply.persisted?
          log.puts("Skipping row #{row_number}: Failed to add reply to topic '#{title}'")
          topic.destroy
          post.destroy
          topic.tags.each(&:destroy) 
          next
        end

        success_count += 1
        progressbar.increment

      rescue StandardError => e
        log.puts("Error processing row #{row_number}: #{e.message}")
      ensure
      end
    end
  end

  Rails.logger.info("\nImport Completed: #{success_count}/#{total_rows} rows successfully imported.")
end

Rails.logger.info("Starting topic import from #{CSV_FILE} file")

previous_log_level = Rails.logger.level
Rails.logger.level = Logger::FATAL

begin
  RateLimiter.disable
  headers = CSV.foreach(CSV_FILE).first
  puts headers
  import_topics_from_csv(CSV_FILE, LOG_FILE)
ensure
  Rails.logger.level = previous_log_level
  RateLimiter.enable
end
Rails.logger.info("CSV import completed. Check the log file at #{LOG_FILE} for errors.")
