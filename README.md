## Section 3 b. Steps to Run

  Twitter Steaming and Batch Data Ingestion

    0. Change directory to Kafka source code
      cd ~/datascience/kafka_2.12-1.0.1

    1.  Start Kafka zookeeper

      bin/zookeeper-server-start.sh config/zookeeper.properties

    2. Start kafka server

      bin/kafka-server-start.sh config/server.properties

    3. Create a topic named "twitterstream" in kafka. The topic's name must be the same with the code
       
       bin/kafka-topics.sh --create --zookeeper --partitions 1 --topic twitterstream localhost:2181 --replication-factor 1

    4. To check if the data is landing in Kafka

      bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic twitterstream --from-beginning

    5. Restful Tweets

      > cd ~/datascience/Twitter01
      > nohup python extract_tweets_restful.py -q searchquerry -l limittweets

    6. Streaming Tweets

      > nohup python extract_tweets_streaming.py -q searchquerry

    7. Define a tweet data table in postgres

      > python create_tweets_table.sql

    8. Launch kafka consumer

      > nohup python kafka_Consumer.py &
