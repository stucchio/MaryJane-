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
  Timestamp sync(1:string streamname) throws (1:MaryJaneException mje, 2:MaryJaneStreamNotFoundException mje2),
  Timestamp addRecord(1:string streamname, 2:string key, 3:string value) throws (1:MaryJaneException mje, 2:MaryJaneStreamNotFoundException mje2, 3:MaryJaneFormatException mje3),
  Timestamp syncAll() throws (1:MaryJaneException mje),
}
