namespace java org.styloot.maryjane.gen

typedef i64 Timestamp

service MaryJane {
  Timestamp sync(1:string streamname),
  Timestamp addRecord(1:string key, 2:string value),
}
