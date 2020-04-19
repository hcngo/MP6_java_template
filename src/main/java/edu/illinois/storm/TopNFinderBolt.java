package edu.illinois.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/** a bolt that finds the top n words. */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;

  NavigableMap<Long, Set<String>> words = new TreeMap<>();
  int topN;


  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  public TopNFinderBolt withNProperties(int N) {
    this.topN = N;
    return this;
  }

  @Override
  public void execute(Tuple tuple) {
    /* ----------------------TODO-----------------------
    Task: keep track of the top N words
		Hint: implement efficient algorithm so that it won't be shutdown before task finished
		      the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
    ------------------------------------------------- */

    String word = tuple.getStringByField("word");
    if (word.isEmpty()) {
      return;
    }
    Long count = tuple.getLongByField("count");

    if (!words.containsKey(count)) {
      words.put(count, new TreeSet<>());
    }

    words.get(count).add(word);

    List<String> topWords = new ArrayList<>();

    for (Long c: words.descendingKeySet()) {
      if (words.get(c).size() >= topN) {
        topWords = words.get(c).stream().collect(Collectors.toList()).subList(0, topN);
        break;
      }
    }

    String output = String.join(", ", topWords);

    System.out.println("top words String: " + output);

    collector.emit(new Values(output));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define output fields
		Hint: there's no requirement on sequence;
					For example, for top 3 words set ("hello", "word", "cs498"),
					"hello, world, cs498" and "world, cs498, hello" are all correct
    ------------------------------------------------- */

    declarer.declare(new Fields("topWordsString"));
  }

}
