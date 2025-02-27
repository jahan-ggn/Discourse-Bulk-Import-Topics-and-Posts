require 'csv'
require 'time'
require 'ruby-progressbar'

# Path to CSV file
CSV_FILE = '/Users/apple/Desktop/Bulk Import Script/Topic_Importer_Data.csv'
LOG_FILE = '/Users/apple/Desktop/Bulk Import Script/import_errors.log'

# Function to parse date from CSV
def parse_datetime(datetime_str)
  return nil if datetime_str.nil? || datetime_str.strip.empty?
  
  begin
    Time.strptime(datetime_str, '%d/%m/%Y %H:%M')
  rescue ArgumentError
    nil
  end
end

# Ensure tags exist before assigning to topics
def ensure_tags_exist(tags)
  return [] if tags.nil? || tags.empty?

  existing_tags = Tag.where(name: tags).pluck(:name)
  new_tags = tags - existing_tags

  new_tags.each do |tag_name|
    tag = Tag.create(name: tag_name)
    if tag.persisted?
      puts "Created new tag: #{tag_name}"
    else
      puts "Failed to create tag: #{tag_name} - #{tag.errors.full_messages.join(', ')}"
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
      begin
        # Extract and sanitize fields
        title = row['Topic_Title']&.strip
        content = row['Topic_Main_Post']&.strip
        creator_email = row['Topic_Main_Post_User_Email']&.strip
        category_id = row['Topic_Category_ID']&.to_i
        tags = row['Topic_Tags']&.split('|').map(&:strip).reject(&:empty?) || []
        created_at = parse_datetime(row['Topic_Main_Post_DateTime']&.strip)

        # Extract reply details
        post_reply = row['Topic_Post_Answer']&.strip
        post_reply_email = row['Topic_Post_Answer_User_Email']&.strip
        post_reply_created_at = parse_datetime(row['Topic_Post_Answer_DateTime']&.strip)

        # Validate required fields and log row number if skipped
        missing_fields = []
        missing_fields << "Topic_Title" if title.nil? || title.empty?
        missing_fields << "Topic_Main_Post" if content.nil? || content.empty?
        missing_fields << "Topic_Main_Post_User_Email" if creator_email.nil? || creator_email.empty?
        missing_fields << "Topic_Category_ID" if category_id.nil? || category_id.zero? || !Category.exists?(id: category_id)
        missing_fields << "Topic_Main_Post_DateTime" if created_at.nil?
        missing_fields << "Topic_Post_Answer" if post_reply.nil? || post_reply.empty?
        missing_fields << "Topic_Post_Answer_User_Email" if post_reply_email.nil? || post_reply_email.empty?
        missing_fields << "Topic_Post_Answer_DateTime" if post_reply_created_at.nil?

        # Skip entire row if any field is missing
        if missing_fields.any?
          log.puts("Skipping row #{row_number}: Missing #{missing_fields.join(', ')}")
          next
        end

        # Fetch users
        user = find_user_by_email(creator_email)
        reply_user = find_user_by_email(post_reply_email)

        if user.nil? || reply_user.nil?
          log.puts("Skipping row #{row_number}: User not found - #{creator_email} or #{post_reply_email}")
          next
        end

        tags = ensure_tags_exist(tags)

        topic_options = {
          title: title,
          raw: content,
          category: category_id,
          tags: tags,
          created_at: created_at,
          import_mode: true,
        }

        topic = TopicCreator.create(user, Guardian.new(user), topic_options)

        if !topic.persisted?
          log.puts("Skipping row #{row_number}: Failed to create topic '#{title}'")
          next
        end

        DiscourseTagging.tag_topic_by_names(topic, user.guardian, tags) if tags.any?

        post = PostCreator.create(user, {
          topic_id: topic.id,
          raw: content,
          created_at: created_at
        })

        if !post.persisted?
          log.puts("Skipping row #{row_number}: Failed to create main post for topic '#{title}'")
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

  puts "\nImport Completed: #{success_count}/#{total_rows} rows successfully imported."
end

Rails.logger.info("Starting topic import from CSV...")

previous_log_level = Rails.logger.level
Rails.logger.level = Logger::FATAL

begin
  RateLimiter.disable
  import_topics_from_csv(CSV_FILE, LOG_FILE)
ensure
  Rails.logger.level = previous_log_level
  RateLimiter.enable
end
Rails.logger.info("CSV import completed. Check the log file at #{LOG_FILE} for errors.")
