namespace java org.styloot.maryjane.gen

typedef i64 Timestamp

exception MaryJaneException {
  1: string error_msg
}


service MaryJane {
  Timestamp sync(1:string streamname) throws (1:MaryJaneException mje),
  Timestamp addRecord(1:string streamname, 2:string key, 3:string value) throws (1:MaryJaneException mje),
}
