package org.kiji.scratch

import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.JobContext

class TestOutputCommitter extends FileOutputCommitter {
  override def commitJob(context: JobContext): Unit = {
    println("I am the custom committer!")
    throw new RuntimeException("I AM THE CUSTOM COMMITTER!")
    super.commitJob(context)
  }
}
