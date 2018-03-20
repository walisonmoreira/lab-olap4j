package lab.olap4j;

import java.sql.DriverManager;

import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Member;

public class Main {

  public static void main(String[] args) throws Exception {
    String driverClass = "org.olap4j.driver.xmla.XmlaOlap4jDriver";
    String jdbcUrl = "jdbc:xmla:Server=http://olap-server/OLAP/msmdpump.dll;Catalog=MyBI";

    System.out.println("Carregando o driver...");
    Class.forName(driverClass);
    System.out.println("Abrindo conexão com o servidor OLAP...");
    OlapConnection olapConnection = (OlapConnection) DriverManager.getConnection(jdbcUrl).unwrap(OlapConnection.class);
    OlapStatement stmt = olapConnection.createStatement();
    
    String olapQuery = ""
      + "SELECT {"
      + "  [Measures].[Carregamento - Valor Total],"
      + "  [Measures].[Carregamento - Volume do Veículo]"
      + "} ON Columns, {"
      + "  [Filial].[Filial]"
      + "} ON Rows FROM"
      + "  [Logistica]";
    System.out.println("Executando expressão MDX...");
    CellSet cellSet = stmt.executeOlapQuery(olapQuery);
    System.out.println("Percorrendo e mostrando o resultado...");
    for (Position row : cellSet.getAxes().get(1)) {
      for (Position column : cellSet.getAxes().get(0)) {
        for (Member member : row.getMembers()) {
          System.out.println(member.getUniqueName());
        }
        for (Member member : column.getMembers()) {
          System.out.println(member.getUniqueName());
        }
        final Cell cell = cellSet.getCell(column, row);
        System.out.println(cell.getFormattedValue());
        System.out.println();
      }
    }
  }
}
