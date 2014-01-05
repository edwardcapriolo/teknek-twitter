teknek-twitter
------

What streaming platform would be complete without something to stream from twitter..

    Map<String,Object> x = MapBuilder.makeMap(TwitterStreamFeed.CONSUMER_KEY,"s",
            TwitterStreamFeed.CONSUMER_SECRET,"B",
            TwitterStreamFeed.TOKEN, "1",
            TwitterStreamFeed.SECRET, "j");
    TwitterStreamFeed sf = new TwitterStreamFeed(x);
    
    
