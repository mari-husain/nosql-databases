require 'redis'

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432

def article_vote(redis, user, article)
  cutoff = Time.now - ONE_WEEK_IN_SECONDS

  unless Time.at(redis.zscore('time:', article)) < cutoff
    article_id = article.split(':')[-1]
    if redis.sadd('voted:' + article_id, user)
      redis.zincrby('score:', VOTE_SCORE, article)
      redis.hincrby(article, 'votes', 1)
    end
  end
end

def article_switch_vote(redis, user, from_article, to_article)
  # HOMEWORK 2 Part I
  cutoff = Time.now - ONE_WEEK_IN_SECONDS

  unless (Time.at(redis.zscore('time:', from_article)) < cutoff or Time.at(redis.zscore('time:', to_article)) < cutoff)
    from_article_id = from_article.split(':')[-1]
    to_article_id = to_article.split(':')[-1]

    # check if we CAN switch the votes
    if redis.smove('voted:' + from_article_id, 'voted:' + to_article_id, user)
      # decrement the number of votes and the score for the from_article
      redis.zincrby('score:', -1 * VOTE_SCORE, from_article)
      redis.hincrby(from_article, 'votes', -1)

      # increment the number of votes and the score for the to_article
      redis.zincrby('score:', VOTE_SCORE, to_article)
      redis.hincrby(to_article, 'votes', 1)
    end
  end
end

redis = Redis.new
# user:3 up votes article:1
article_vote(redis, "user:3", "article:1")
# user:3 up votes article:3
article_vote(redis, "user:3", "article:3")
# user:2 switches their vote from article:8 to article:1
article_switch_vote(redis, "user:2", "article:8", "article:1")

# Which article's score is between 10 and 20?
# PRINT THE ARTICLE'S LINK TO STDOUT:
# HOMEWORK 2 Part II
article = redis.zrangebyscore('score:', 10, 20)[0]
puts redis.hget(article, 'link')