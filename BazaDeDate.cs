using System.Data.OleDb;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using MySqlConnector;
namespace InMariaDb;

public static class BazaDeDate
{
    internal static void scrieSqlFile(string linie)
    {
        using (StreamWriter writer = new StreamWriter(AppContext.BaseDirectory + "\\script.sql", true))
        {
            writer.WriteLine(linie);
        }
    }
    internal static void conectareAccesDb(Action<OleDbDataReader> actiune)
    {
        OleDbConnectionStringBuilder csb = new OleDbConnectionStringBuilder();
        csb.Provider = "Microsoft.ACE.OLEDB.12.0";
        csb.DataSource = AppContext.BaseDirectory + "\\Data\\2026.mdb";
        using (OleDbConnection cnn = new OleDbConnection(csb.ConnectionString))
        {
            cnn.Open();
            string slq = "SELECT * FROM adrrol;";
            using (OleDbCommand cmd = new OleDbCommand(slq, cnn))
            {
                using (OleDbDataReader dr = cmd.ExecuteReader())
                {
                    actiune(dr);
                }
            }
        }
    }
    internal static int getCodLocalitate(string localitate)
    {
        MySqlConnectionStringBuilder csb = new MySqlConnectionStringBuilder();
        csb.Server = "127.0.0.1";
        csb.Port = 3306;
        csb.Database = "borca_2026";
        csb.UserID = "appuser";
        csb.Password = "ctce";
        using (MySqlConnection cnn = new MySqlConnection(csb.ConnectionString))
        {
            cnn.Open();
            if (localitate == "PARAUL PANTEI")
            {
                localitate = "PÂRÂUL PÂNTEI";
            }
            if (localitate == "PARAUL CARJEI")
            {
                localitate = "PÂRÂUL CÂRJEI";
            }
            string slq = "SELECT `cod` FROM `cfg_localitati` WHERE localitate = '" + localitate + "';";
            using (MySqlCommand cmd = new MySqlCommand(slq, cnn))
            {
                using (MySqlDataReader dr = cmd.ExecuteReader())
                {
                    if (dr.Read())
                    {
                        return dr.GetInt16("cod");
                    }else
                    {
                        return 0;
                    }
                }
            }
        }
    }

     internal static int getCodExploatatie(string exploatatie)
    {
        OleDbConnectionStringBuilder csb = new OleDbConnectionStringBuilder();
        csb.Provider = "Microsoft.ACE.OLEDB.12.0";
        csb.DataSource = AppContext.BaseDirectory + "\\Data\\2026.mdb";
        using (OleDbConnection cnn = new OleDbConnection(csb.ConnectionString))
        {
            cnn.Open();
            if (exploatatie == "..." | exploatatie =="")
            {
                exploatatie = "1. Gospodarie/Exploatatie agricola individuala";
            }
            string slq = "SELECT id FROM `exploatatie` WHERE denumire = '" + exploatatie + "';";
            using (OleDbCommand cmd = new OleDbCommand(slq, cnn))
            {
                using (OleDbDataReader dr = cmd.ExecuteReader())
                {
                    if (dr.Read())
                    {
                        return Convert.ToInt16(dr["id"]);
                    }else
                    {
                        return 0;
                    }
                }
            }
        }
    }

    internal static string getSiruta(string sat)
    {
        OleDbConnectionStringBuilder csb = new OleDbConnectionStringBuilder();
        csb.Provider = "Microsoft.ACE.OLEDB.12.0";
        csb.DataSource = AppContext.BaseDirectory + "\\Data\\2026.mdb";
        using (OleDbConnection cnn = new OleDbConnection(csb.ConnectionString))
        {
            cnn.Open();
            string slq = "SELECT codsiruta FROM `localitati` WHERE denumire = '" + sat + "';";
            using (OleDbCommand cmd = new OleDbCommand(slq, cnn))
            {
                using (OleDbDataReader dr = cmd.ExecuteReader())
                {
                    if (dr.Read())
                    {
                        int ord_codsiruta = dr.GetOrdinal("codsiruta");
                        string codsiruta = dr.IsDBNull(ord_codsiruta)
                                ? ""
                                : dr.GetString(ord_codsiruta);
                        return codsiruta;
                    }else
                    {
                        return "";
                    }
                }
            }
        }
    }

    internal static int ExecutaNonQuery(string sql)
        {
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder();
            builder.Server = "127.0.0.1";
            builder.Port = 3306;
            builder.Database = "borca_2026";
            builder.UserID = "appuser";
            builder.Password = "ctce";
            var ret = 0;
            using (var conn = new MySqlConnection(builder.ConnectionString))
            {
                conn.Open();
                using (var cmd = new MySqlCommand())
                {
                    try
                    {
                        cmd.Connection = conn;
                        cmd.CommandText = sql;
                        ret = cmd.ExecuteNonQuery();
                        return ret;
                    }
                    catch(MySqlException ex)
                    {
                        Console.WriteLine(ex.Message);
                        return ret;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        return ret;
                        throw;
                    }
                }
            }
        }

    internal static void ConstruiesteInsert()
    {
        string? sqlRol;
        string? sqlAdrGos;
        string? sqlAdrPf = "";
        string? sqlAdrPj = "";
        string? sqlPer = "";
        string? sqlPf;
        string? sqlPj;
        bool isPf = false;

        string? idAdresaGos;
        string? idPersoane;
        string? idAdresaPf = "";
        string? idAdresaPj = "";
        string? idPersoana;

        string? UAT;
        int result = 0;

        string? input;
        string? numar;

        int latimePj;
        string formaOrgPj="";
        string numePj;
        int codFormaOrganizare;

        UAT = "BORCA";

        if (File.Exists(AppContext.BaseDirectory + "\\script.sql"))
        {
            File.Delete(AppContext.BaseDirectory + "\\script.sql");
        }

        BazaDeDate.conectareAccesDb(async dr =>
        {
            while (dr.Read())
            {
                idAdresaGos = Guid.NewGuid().ToString();
                if (dr.GetInt32(dr.GetOrdinal("tip")) == 1 || dr.GetInt32(dr.GetOrdinal("tip")) == 2)
                {
                    isPf =true;
                    
                }else if (dr.GetInt32(dr.GetOrdinal("tip")) == 3 || dr.GetInt32(dr.GetOrdinal("tip")) == 4)
                {
                    isPf = false;

                }
                else
                {
                    throw new Exception("Tip invalid");
                }

                if (isPf)
                {
                    idAdresaPf = Guid.NewGuid().ToString();
                }
                else
                {
                     idAdresaPj = Guid.NewGuid().ToString();
                }
                idPersoane = Guid.NewGuid().ToString();
                idPersoana = Guid.NewGuid().ToString();
               
                sqlRol = "INSERT INTO `adrese_roluri`";
                sqlRol += "(";
                sqlRol += " `id`";
                sqlRol += ", `cod_cfg_localitati`";
                sqlRol += ", `volum`";
                sqlRol += ", `pozitie`";
                sqlRol += ", `id_adresa_rol`";
                sqlRol += ", `cod_cfg_exploatatii`";
                sqlRol += ", `id_persoana`";
                sqlRol += ", `rol_impozite`";
                sqlRol += ", `data_declaratie`";
                sqlRol += ", `nr_inregistrare`";
                sqlRol += ", `data_inregistrare`";
                sqlRol += ", `semnat`";
                sqlRol += ", `anulat`";
                sqlRol += "	)VALUES(";
                sqlRol += "'" + Guid.NewGuid() + "'";

                sqlRol += ", " + BazaDeDate.getCodLocalitate(
                        dr.IsDBNull(dr.GetOrdinal("localitate")) ? "NECUNOSCUT" : dr.GetString(dr.GetOrdinal("localitate"))
                    ) + "";

                sqlRol += ", " + dr["vol"] + "";
                sqlRol += ", " + dr["poz"] + "";
                sqlRol += ", '" + idAdresaGos + "'";

                sqlRol += ", " + BazaDeDate.getCodExploatatie(
                     dr.IsDBNull(dr.GetOrdinal("tipexploa")) ? "NECUNOSCUT" : dr.GetString(dr.GetOrdinal("tipexploa"))
                ) + "";
                sqlRol += ", '" + idPersoane + "'";
                sqlRol += ", '" + dr["rolIMP"] + "'";
                sqlRol += ", '" + "0000-00-00" + "'";
                sqlRol += ", '" + dr["nrinreg"] + "'";
                sqlRol += ", '" + "0000-00-00" + "'";
                
                if ((dr.IsDBNull(dr.GetOrdinal("semnat")) ? "NU" : dr.GetString(dr.GetOrdinal("semnat"))) == "DA")
                {
                    sqlRol += ", " + "0" + "";
                }else
                {
                    sqlRol += ", " + "1" + "";
                }
                sqlRol += ", " + "NULL" + "";
                sqlRol += ");";

                sqlAdrGos = "INSERT INTO `adrese`";
                sqlAdrGos += "(";
                sqlAdrGos += " `id`";
                sqlAdrGos += ", `tip`";
                sqlAdrGos += ", `judet`";
                sqlAdrGos += ", `localitate`";
                sqlAdrGos += ", `zona`";
                sqlAdrGos += ", `strada`";
                sqlAdrGos += ", `numar`";
                sqlAdrGos += ", `litera`";
                sqlAdrGos += ", `bloc`";
                sqlAdrGos += ", `scara`";
                sqlAdrGos += ", `etaj`";
                sqlAdrGos += ", `apartament`";
                sqlAdrGos += ", `cod_postal`";
                sqlAdrGos += ", `cod_siruta`";
                sqlAdrGos += ")VALUES(";
                if (isPf)
                {
                    sqlAdrPf = sqlAdrGos;
                }else if (!isPf)
                {
                    sqlAdrPj = sqlAdrGos;
                }

                sqlAdrGos += " '" + idAdresaGos + "'";
                if (isPf)
                {
                    sqlAdrPf += " '" + idAdresaPf + "'";
                }else if (!isPf)
                {
                    sqlAdrPj += " '" + idAdresaPj + "'";
                }

                sqlAdrGos += ", '" + "GOS" + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + "PF" + "'";
                }else if (!isPf){
                    sqlAdrPj += ", '" + "PJ" + "'";
                }

                sqlAdrGos += ", '" + dr.GetString(dr.GetOrdinal("judet")) + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr.GetString(dr.GetOrdinal("judet")) + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr.GetString(dr.GetOrdinal("judet")) + "'";
                }

                sqlAdrGos += ", '" + UAT + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + UAT + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + UAT + "'";
                }

                sqlAdrGos += ", '" + dr["localitate"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["localitate"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["localitate"] + "'";
                }
                
                sqlAdrGos += ", '" + dr["strada"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["strada"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["strada"] + "'";
                }

                input = dr.GetString(dr.GetOrdinal("nr"));
                numar = new string(input.Where(char.IsDigit).ToArray());
                if (numar is null || numar == "")
                {
                    numar = "0";
                }
                sqlAdrGos += ", " + numar + "";
                if (isPf)
                {
                    sqlAdrPf += ", " + numar + "";
                }
                else
                {
                    sqlAdrPj += ", " + numar + "";
                }

                sqlAdrGos += ", '" + dr["litera"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["litera"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["litera"] + "'";
                }

                sqlAdrGos += ", '" + dr["bloc"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["bloc"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["bloc"] + "'";
                }

                sqlAdrGos += ", '" + dr["scara"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["scara"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["scara"] + "'";
                }

                sqlAdrGos += ", '" + dr["etaj"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["etaj"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["etaj"] + "'";
                }

                sqlAdrGos += ", '" + dr["ap"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["ap"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["ap"] + "'";
                }

                sqlAdrGos += ", '" + dr["codposta"] + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + dr["codposta"] + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + dr["codposta"] + "'";
                }

                sqlAdrGos += ", '" + getSiruta( 
                    dr.IsDBNull(dr.GetOrdinal("localitate")) ? "NECUNOSCUT" : dr.GetString(dr.GetOrdinal("localitate"))
                ) + "'";
                if (isPf)
                {
                    sqlAdrPf += ", '" + getSiruta( 
                        dr.IsDBNull(dr.GetOrdinal("localitate")) ? "NECUNOSCUT" : dr.GetString(dr.GetOrdinal("localitate"))
                    ) + "'";
                }
                else
                {
                    sqlAdrPj += ", '" + getSiruta( 
                        dr.IsDBNull(dr.GetOrdinal("localitate")) ? "NECUNOSCUT" : dr.GetString(dr.GetOrdinal("localitate"))
                    ) + "'";
                }
                
                sqlAdrGos += ");";
                if (isPf)
                {
                    sqlAdrPf += ");";
                }
                else
                {
                    sqlAdrPj += ");";
                }

                Console.WriteLine(sqlRol);
                scrieSqlFile(sqlRol);
                //result = BazaDeDate.ExecutaNonQuery(sqlRol);
                sqlRol = string.Empty;

                Console.WriteLine(sqlAdrGos);
                scrieSqlFile(sqlAdrGos);
                //result = BazaDeDate.ExecutaNonQuery(sqlAdrGos);
                sqlAdrGos = string.Empty;

                

                if (isPf)
                {
                    Console.WriteLine(sqlAdrPf);
                    scrieSqlFile(sqlAdrPf);
                    //result = BazaDeDate.ExecutaNonQuery(sqlAdrPf);
                    sqlAdrPf = string.Empty;
                }
                else
                {
                    Console.WriteLine(sqlAdrPj);
                    scrieSqlFile(sqlAdrPj);
                    //result = BazaDeDate.ExecutaNonQuery(sqlAdrPj);
                    sqlAdrPf = string.Empty;
                }

                sqlPer += "INSERT INTO `persoane` (";
                sqlPer += "	`id`";
                sqlPer += "	, `tip`";
                sqlPer += "	, `id_persoana`";
                sqlPer += ") VALUES (";
                sqlPer += "	'" + idPersoane + "'";
                sqlPer += "	, " + dr.GetInt32(dr.GetOrdinal("tip")) + "";
                sqlPer += "	, '" + idPersoana + "'";
                sqlPer += ");";
                Console.WriteLine(sqlPer);
                scrieSqlFile(sqlPer);
                //result = BazaDeDate.ExecutaNonQuery(sqlPer);
                sqlPer = string.Empty;

                if (isPf)
                {
                    sqlPf = "INSERT INTO `persoane_fizice` ";
                    sqlPf += "(";
                    sqlPf += "`id`";
                    sqlPf += ", `cnp`";
                    sqlPf += ", `sex`";
                    sqlPf += ", `data_nasterii`";
                    sqlPf += ", `nume`";
                    sqlPf += ", `initiala`";
                    sqlPf += ", `prenume`";
                    sqlPf += ", `email`";
                    sqlPf += ", `telefon`";
                    sqlPf += ", `buletin`";
                    sqlPf += ", `id_adrese`";
                    sqlPf += ") VALUES (";
                    sqlPf += "'" + idPersoana + "'";
                    sqlPf += ", '" + dr.GetString(dr.GetOrdinal("cnp")) + "'";
                    sqlPf += ", " + "0" + "";
                    sqlPf += ", '0000-00-00'";
                    sqlPf += ", '" + dr.GetString(dr.GetOrdinal("nume")) + "'";
                    sqlPf += ", '" + dr.GetString(dr.GetOrdinal("sirues")) + "'";
                    sqlPf += ", '" + dr.GetString(dr.GetOrdinal("prenume")) + "'";
                    sqlPf += ", ''";
                    sqlPf += ", ''";
                    sqlPf += ", ''";
                    sqlPf += ", '" + idAdresaPf + "'";
                    sqlPf += ");";

                    Console.WriteLine(sqlPf);
                    scrieSqlFile(sqlPf);
                    //result = BazaDeDate.ExecutaNonQuery(sqlPf);
                    sqlPf = string.Empty;
                }
                else
                {
                    sqlPj = "INSERT INTO `persoane_juridice` ";
                    sqlPj += "(`id`";
                    sqlPj += ", `cod_forma_organizare`";
                    sqlPj += ", `denumire`";
                    sqlPj += ", `filiala`";
                    sqlPj += ", `cif`";
                    sqlPj += ", `cui`";
                    sqlPj += ", `registrul_comertului`";
                    sqlPj += ", `nume_reprezentant`";
                    sqlPj += ", `intiala_reprezenant`";
                    sqlPj += ", `prenume_reprezentant`";
                    sqlPj += ", `functia`";
                    sqlPj += ", `telefon`";
                    sqlPj += ", `email`";
                    sqlPj += ", `id_adrese`";
                    sqlPj += ") VALUES (";
                    sqlPj += "'" + idPersoana + "'";

                    latimePj = dr.GetString(dr.GetOrdinal("nume")).Length;
                    if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-2,2) == "SA")
                    {
                        formaOrgPj = "SA";
                        codFormaOrganizare = 1;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-4).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-2).Trim(); 
                        }
                    }else if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-3,3) == "SRL")
                    {
                        formaOrgPj = "SRL";
                        codFormaOrganizare = 2;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-5).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-3).Trim(); 
                        }
                    }else if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-3,3) == "SNC")
                    {
                        formaOrgPj = "SNC";
                        codFormaOrganizare = 3;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-5).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-3).Trim(); 
                        }
                    }else  if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-3,3) == "SCA")
                    {
                        formaOrgPj = "SCA";
                        codFormaOrganizare = 4;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-5).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-3).Trim(); 
                        }
                    }else if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-3,3) == "SCS")
                    {
                        formaOrgPj = "SCS";
                        codFormaOrganizare = 5;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-5).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-3).Trim(); 
                        }
                    }else if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-2,2) == "IF")
                    {
                        formaOrgPj = "IF";
                        codFormaOrganizare = 6;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-4).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-2).Trim(); 
                        }
                    }else if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-2,2) == "II")
                    {
                        formaOrgPj = "II";
                        codFormaOrganizare = 7;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-4).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-2).Trim(); 
                        }
                    }else if (dr.GetString(dr.GetOrdinal("nume")).Substring(latimePj-3,3) == "PFA")
                    {
                        formaOrgPj = "PFA";
                        codFormaOrganizare = 8;
                        if (dr.GetString(dr.GetOrdinal("nume")).Substring(0,2).ToUpper() == "SC")
                        {
                             numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(2, latimePj-5).Trim();
                        }
                        else
                        {
                            numePj = dr.GetString(dr.GetOrdinal("nume")).Substring(0, latimePj-3).Trim(); 
                        }
                    }
                    else
                    {
                        formaOrgPj = "";
                        codFormaOrganizare = 9;
                        numePj = dr.GetString(dr.GetOrdinal("nume")).Trim(); 
                    }

                    sqlPj += ", " + codFormaOrganizare + "";
                    sqlPj += ", '" + numePj + "'";
                    sqlPj += ", ''";
                    sqlPj += ", '" + dr.GetString(dr.GetOrdinal("cnp")) + "'";
                    sqlPj += ", '" + dr.GetString(dr.GetOrdinal("nrUi")) +  "'";
                    sqlPj += ", ''";
                    sqlPj += ", ''";
                    sqlPj += ", ''";
                    sqlPj += ", ''";
                    sqlPj += ", ''";
                    sqlPj += ", ''";
                    sqlPj += ", ''";
                    sqlPj += ", '" + idAdresaPj + "'";
                    sqlPj += ");";

                    Console.WriteLine(sqlPj);
                    scrieSqlFile(sqlPj);
                    //result = BazaDeDate.ExecutaNonQuery(sqlPj);
                    sqlPj = string.Empty;
                }
            }
            Console.WriteLine("am terminat");
        });
    }
}
