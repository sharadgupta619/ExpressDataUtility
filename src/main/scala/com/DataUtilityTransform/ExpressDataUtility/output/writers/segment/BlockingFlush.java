package com.DataUtilityTransform.ExpressDataUtility.output.writers.segment;

import com.segment.analytics.Analytics;
import com.segment.analytics.Callback;
import com.segment.analytics.MessageTransformer;
import com.segment.analytics.Plugin;
import com.segment.analytics.messages.Message;
import com.segment.analytics.messages.MessageBuilder;
import java.util.concurrent.Phaser;


public class BlockingFlush {

    public static BlockingFlush create() {
        return new BlockingFlush();
    }

    BlockingFlush() {
        this.phaser = new Phaser(1);
    }

    final Phaser phaser;

    public Plugin plugin() {
        return new Plugin() {
            @Override public void configure(Analytics.Builder builder) {
                builder.messageTransformer(new MessageTransformer() {
                    @Override public boolean transform(MessageBuilder builder) {
                        phaser.register();
                        return true;
                    }
                });

                builder.callback(new Callback() {
                    @Override public void success(Message message) {
                        phaser.arrive();
                    }

                    @Override public void failure(Message message, Throwable throwable) {
                        phaser.arrive();
                    }
                });
            }
        };
    }

    public void block() {
        phaser.arriveAndAwaitAdvance();
    }
}