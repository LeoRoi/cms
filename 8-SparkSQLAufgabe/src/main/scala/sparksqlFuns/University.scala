package sparksqlFuns

case class Professor(persnr: Int, name: String, rang: String, raum: Int)
case class Student(matrnr: Int, name: String, semester: Int)
case class Vorlesung(vorlnr: Int, titel: String, sws: Int, gelesenVon: Int)
case class hoeren(matrnr: Int, vorlnr: Int)
case class voraussetzen(vorgaenger: Int, nachfolger: Int)
case class Assistenten(persnr: Int, name: String, fachgebiet: String, boss: Int)
case class pruefen(matrnr: Int, vorlnr: Int, persnr: Int, note: Double)

trait University {
  val data_profs = List(Professor(2125, "Sokrates", "C4", 226), Professor(2126, "Russel", "C4", 232),
    Professor(2127, "Kopernikus", "C3", 310), Professor(2133, "Popper", "C3", 52),
    Professor(2134, "Augustinus", "C3", 309), Professor(2136, "Curie", "C4", 36),
    Professor(2137, "Kant", "C4", 7))

  val data_studenten = List(Student(24002, "Xenokrates", 18), Student(25403, "Jonas", 12),
    Student(26120, "Fichte", 10), Student(26830, "Aristoxenos", 8),
    Student(27550, "Schopenhauer", 6), Student(28106, "Carnap", 3),
    Student(29120, "Theophrastos", 2), Student(29555, "Feuerbach", 2))

  val data_vorlesungen = List(Vorlesung(5001, "Grundzuege", 4, 2137), Vorlesung(5041, "Ethik", 4, 2125),
    Vorlesung(5043, "Erkenntnistheorie", 3, 2126), Vorlesung(5049, "Maeeutik", 2, 2125),
    Vorlesung(4052, "Logik", 4, 2125), Vorlesung(5052, "Wissenschaftstheorie", 3, 2126),
    Vorlesung(5216, "Bioethik", 2, 2126), Vorlesung(5259, "Der Wiener Kreis", 2, 2133),
    Vorlesung(5022, "Glaube und Wissen", 2, 2134), Vorlesung(4630, "Die 3 Kritiken", 4, 2137))

  val data_hoeren = List(hoeren(26120, 5001), hoeren(27550, 5001), hoeren(27550, 4052), hoeren(28106, 5041),
    hoeren(28106, 5052), hoeren(28106, 5216), hoeren(28106, 5259), hoeren(29120, 5001), hoeren(29120, 5041),
    hoeren(29120, 5049), hoeren(29555, 5022), hoeren(25403, 5022), hoeren(29555, 5001))

  val data_voraussetzen = List(voraussetzen(5001, 5041), voraussetzen(5001, 5043), voraussetzen(5001, 5049),
    voraussetzen(5041, 5216), voraussetzen(5043, 5052), voraussetzen(5041, 5052), voraussetzen(5052, 5259))

  val data_assis = List(Assistenten(3002, "Platon", "Ideenlehre", 2125), Assistenten(3003, "Aristoteles", "Syllogistik", 2125),
    Assistenten(3004, "Wittgenstein", "Sprachtheorie", 2126), Assistenten(3005, "Rhetikus", "Planetenbewegung", 2127),
    Assistenten(3006, "Newton", "Keplersche Gesetze", 2127), Assistenten(3007, "Spinoza", "Gott und Natur", 2134))

  val data_pruefen = List(pruefen(28106, 5001, 2126, 1.0), pruefen(25403, 5041, 2125, 2.0),
    pruefen(27550, 4630, 2137, 2.0))
  val vorlnummern = Array(5041, 5043, 4052, 5052, 5216, 5259, 5022, 4630)
  val studinummern = Array(24002, 25401, 26120, 26830, 27550, 28106, 29120, 29555)
  val rand = new scala.util.Random
  val hoeren_big = hoeren(28106, 5001) :: (for (i <- 1 to 1000000) yield {
    val v = rand.nextInt(8)
    val s = rand.nextInt(8)
    hoeren(s, v)
  }).toList
}