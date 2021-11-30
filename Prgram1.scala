import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.io._
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.functions._


object Principal_Program {

  def service_To_choose():Unit={

    println("Merci de choisir l'un des services ci-dessous")
    println("Service 1: Un service de supprission des données depuis le fichier CSV, " + "pour choisr ce service merci de taper S1");
    println("Service 2: Un service de hashage des données d'un client lorsqu'il le demande," + " pour choisr ce service merci de taper S2");
    println("Service 3: Un service permet de générer un fichier CSV contenant toutes les données clients, " + "et les lui envoyer par email, pour choisr ce service merci de taper S3");

  }

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }

  def msg_erreur(): Unit = {

    println("erreur de saisi!!! Merci de choisir l'un des services ci-dessous")
    service_To_choose()

  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val session = SparkSession.builder().master("local").getOrCreate()
    val sc = session.sparkContext

    // preprocessing
    val data = sc.textFile("/home/zunu/Bureau/zunu.csv")
    val header = data.first()
    val data_without_header = data.filter(e => e != header)
    val dataset1 = data_without_header.map(x => x.split(","))
    var dataset = dataset1.map(x => Transaction(x(0).toString, x(1).toString, x(2).toString, x(3).toDouble,
      x(4).toString, x(5).toString))

    // choose the service
    service_To_choose()
    while(true) {

    val service = scala.io.StdIn.readLine()

      // services
      service match {
        case "S1" => {

          println("Service 1, Id à supprimer : ");

          val identifiant = scala.io.StdIn.readLine();
          val filt_Ident = dataset.filter(x => x.ID.!=(identifiant));
          val path = "/home/zunu/Service1"
          filt_Ident.foreach(u => println(u));

          if (Files.exists(Paths.get(path ++ "_Without_" ++ identifiant)) == false) {
          filt_Ident.coalesce(1, true).saveAsTextFile(path ++ "_Without_" ++ identifiant);}

          println("Service 1 ..........................................................")
          println("les données qui correspondent à " ++ identifiant ++ " sont supprimées")
          println("le nouveau Dataset est stocké ici: " ++ path ++ "_Without_" ++ identifiant)

          dataset=filt_Ident

          service_To_choose()
        }
        case "S2" => {

          import session.sqlContext.implicits._

          println("Service 2, Identifiant de l'utilisateur pour hasher ses données ? ")
          val identifiant = scala.io.StdIn.readLine()
          val path = "/home/zunu/Service2"

          // hashage des données
          val withHashedColumn = dataset.toDF().withColumn("Hashed Data", hash($"ID"))
          if (Files.exists(Paths.get(path ++ "_WithHashedColumn" ++ identifiant)) == false) {
          withHashedColumn.coalesce(1).write.option("header", "true").csv(path ++ "_WithHashedColumn" ++ identifiant)}
          println("hashage Dataset!! ,chemin =>" ++ path ++ "_WithHashedColumn" ++ identifiant)

          // créer un dataset de l'utilisateur identifié
          val ClientHashedData = withHashedColumn.filter(withHashedColumn.col("ID").equalTo(identifiant))
          if (Files.exists(Paths.get(path ++ "_WithHashedData_Of_" ++ identifiant)) == false) {
          ClientHashedData.coalesce(1).write.option("header", "true").csv(path ++ "_WithHashedData_Of_" ++ identifiant)}
          println("hashage données qui correspondent à " ++ identifiant ++ "!! ,chemin =>" ++ path ++ "_WithHashedColumn" ++ identifiant)

          service_To_choose()

        }

        case "S3" => {

          import Mail._

          println("Service 3, Identifiant de l'utilisateur pour lui envoyer un rapport de ses achats ? ")

          val identifiant = scala.io.StdIn.readLine()
          val path = "/home/zunu/Service3"
          val filt_Ident = dataset.filter(x => x.ID.equals(identifiant));

          if (Files.exists(Paths.get(path + "_" + identifiant)) == false) {
          filt_Ident.coalesce(1, true).saveAsTextFile(path + "_" + identifiant);}
          println("fichier historique des achat est enregistré sous le chemin suivant : " + path)

          val email_adress = filt_Ident.map(x => x.EMAIL.toString).first()
          val name_person = filt_Ident.map(x => x.F_NAME.toString).first()

          send a new Mail(
            from = "Zainabmessrar99@gmail.com" -> "Messrar Zineb",
            to = Seq(email_adress),
            subject = "Confirmation Transaction Achat",
            message = "Bonjour " + name_person + ", \n\n" +
              "On vous écrit ce présent mail pour vous confirmer vos achats sur notre plateforme.\n\n" +
              "EN vous remerciant pour votre confiance !",
            attachments = new java.io.File(path + "_" + identifiant + "/part-00000")
          )

          println("Service 3 ......................................................................")
          println("un message de confirmation de la transaction est envoyé à " + name_person + " à l'adreese mail suivante: " + email_adress)

          service_To_choose()
        }

        case _ => msg_erreur()

      }
    }
  }

}



