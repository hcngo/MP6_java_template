package edu.illinois.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/** a spout that generate sentences from a file */
public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext _context;
  private String inputFile;
  private BufferedReader reader;

  // Hint: Add necessary instance variables if needed

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this._context = context;
    this._collector = collector;
    try {
      System.out.println("The file is " + inputFile);
      this.reader = new BufferedReader(new FileReader(inputFile));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  // Set input file path
  public FileReaderSpout withInputFileProperties(String inputFile) {
    this.inputFile = inputFile;
    return this;
  }

  @Override
  public void nextTuple() {
    if (reader == null) {
      return;
    }

    boolean stillRead = true;
    while (stillRead) {
      try {
        String line = reader.readLine();
        if (line == null) {
          System.out.println("line is null!");
          stillRead = false;
          continue;
        }

        line = line.trim();
        System.out.println("line: " + line);
        _collector.emit(new Values(line));

      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    Utils.sleep(1000);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        System.out.println("close is called!");
        reader.close();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public void fail(Object msgId) {}

  public void ack(Object msgId) {}

  @Override
  public void activate() {}

  @Override
  public void deactivate() {}

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
