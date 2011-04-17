namespace java org.styloot.maryjane.gen

typedef i64 Timestamp

exception MaryJaneException {
  1: string error_msg
}

exception MaryJaneStreamNotFoundException {
  1: string error_msg
}

exception MaryJaneFormatException {
  1: string error_msg
}

service MaryJane {
  Timestamp addRecord(1:string streamname, 2:string key, 3:string value) throws (1:MaryJaneException mje, 2:MaryJaneStreamNotFoundException mje2, 3:MaryJaneFormatException mje3),
  /* Adds a record to the specified stream. Key and value may not contain either tabs or newlines.
   */
  Timestamp sync(1:string streamname) throws (1:MaryJaneException mje, 2:MaryJaneStreamNotFoundException mje2),
  /* Upload all available data from the stream to Hadoop.*/
  Timestamp syncAll() throws (1:MaryJaneException mje),
   /* Upload all available data in all streams to Hadoop.*/
}
