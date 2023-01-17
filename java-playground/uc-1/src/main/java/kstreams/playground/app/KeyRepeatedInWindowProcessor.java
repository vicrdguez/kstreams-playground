package kstreams.playground.app;

import java.time.Duration;
import ksql.pageviews;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*

 */
public class KeyRepeatedInWindowProcessor implements Processor<String, pageviews, String, pageviews> {

  private static final Logger logger = LoggerFactory.getLogger(KeyRepeatedInWindowProcessor.class);
  private KeyValueStore<String, ValueAndTimestamp<String>> pageIdTsStore;
  private final Duration windowSize;
  private ProcessorContext<String, pageviews> localContext;

  KeyRepeatedInWindowProcessor(Duration windowSize){
    this.windowSize = windowSize;
  }

  @Override
  public void init(ProcessorContext<String, pageviews> context) {
    localContext = context;
    pageIdTsStore = localContext.getStateStore(Constant.PAGE_ID_TS_STORE);
  }

  @Override
  public void process(Record<String, pageviews> record) {
    if (pageIdTsStore.get(record.key()) == null) {
      logger.info("{} Adding UserId: [{}] and PageId: [{}] with Timestamp: [{}] to the store",
          Constant.LOG_PREFIX, record.key(), record.value().getPageid(), record.timestamp());
      pageIdTsStore.put(
          record.key(),
          ValueAndTimestamp.make(record.value().getPageid(), record.timestamp()));
    }else {
      ValueAndTimestamp<String> pageIdAndTs = pageIdTsStore.get(record.key());
      boolean samePageId = pageIdAndTs.value().equals(record.value().getPageid());
      Duration durationOnPage = Duration.ofMillis(record.timestamp() - pageIdAndTs.timestamp());
      boolean atLeastWindowSizeAppart = durationOnPage.compareTo(windowSize) > 0;

      if (samePageId && atLeastWindowSizeAppart){
        logger.info("{} UserId: [{}] was on pageId: [{}] for more than {} ({})m",
            Constant.LOG_PREFIX,
            record.key(),
            pageIdAndTs.value(),
            windowSize.toMinutes(),
            durationOnPage.toMinutes());

        pageIdTsStore.delete(record.key());
        localContext.forward(record);
      }
    }
  }

  @Override
  public void close() {
    Processor.super.close();
  }
}
