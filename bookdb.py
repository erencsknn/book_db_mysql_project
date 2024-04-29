import mysql.connector
from ibookdb import IBOOKDB
from queryresult import QueryResult

class BOOKDB(IBOOKDB):

    def __init__(self,user,password,host,database,port):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.connection = None

    def initialize(self):
        self.connection = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            port=self.port
        )

    def disconnect(self):
        if self.connection is not None:
            self.connection.close()


    def createTables(self):
        cursor = self.connection.cursor()
        cursor.execute("CREATE TABLE author (author_id INT PRIMARY KEY, author_name VARCHAR(60))")
        cursor.execute("CREATE TABLE publisher (publisher_id INT PRIMARY KEY , publisher_name VARCHAR(50))")
        cursor.execute("CREATE TABLE book (isbn CHAR(13) PRIMARY KEY, book_name VARCHAR(120), publisher_id INT , first_publish_year CHAR(4), page_count INT, category VARCHAR(25), rating FLOAT, FOREIGN KEY(publisher_id) REFERENCES publisher(publisher_id) ON DELETE CASCADE)")
        cursor.execute("CREATE TABLE author_of (isbn CHAR(13), author_id INT, FOREIGN KEY(isbn) REFERENCES book(isbn) ON DELETE CASCADE, FOREIGN KEY(author_id) REFERENCES author(author_id) ON DELETE CASCADE)")
        cursor.execute("CREATE TABLE phw1 (isbn CHAR(13) PRIMARY KEY, book_name VARCHAR(120), rating FLOAT)")
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES")
        table_count = len(cursor.fetchall())
        return table_count
       

    def dropTables(self):
        try:
            cursor = self.connection.cursor()
            drop_table_queries = [
                "DROP TABLE IF EXISTS author_of",
                "DROP TABLE IF EXISTS author",
                "DROP TABLE IF EXISTS book",
                "DROP TABLE IF EXISTS publisher",
                "DROP TABLE IF EXISTS phw1"
            ]
            # Her bir SQL sorgusunu çalıştır
            for query in drop_table_queries:
                cursor.execute(query)
            # Değişiklikleri kaydet
            self.connection.commit()
            # Silinen tablo sayısını döndür
            return len(drop_table_queries)
        except mysql.connector.Error as e:
            print("Tablolar silinirken bir hata oluştu:", e)
            return 0
    
        
    def insertAuthor(self,authors):
        try:
            cursor = self.connection.cursor()
            sql = "INSERT INTO author (author_id, author_name) VALUES (%s, %s)"
            for author in authors:
                cursor.execute(sql, (author.author_id, author.author_name))
            self.connection.commit()
            return len(authors)
        except mysql.connector.Error as e:
            print("Veri eklenirken bir hata oluştu:", e)
            return 0
        
      
    def insertBook(self,books):
        try:
            cursor = self.connection.cursor()
            sql = "INSERT INTO book (isbn, book_name, publisher_id, first_publish_year, page_count, category, rating) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            for book in books:
                cursor.execute(sql, (book.isbn, book.book_name, book.publisher_id, book.first_publish_year, book.page_count, book.category, book.rating))
            self.connection.commit()
            return len(books)
        except mysql.connector.Error as e:
            print("Veri eklenirken bir hata oluştu:", e)
            return 0
        
        
    def insertPublisher(self,publishers):
        try:
            cursor = self.connection.cursor()
            sql = "INSERT INTO publisher (publisher_id, publisher_name) VALUES (%s, %s)"
            for publisher in publishers:
                cursor.execute(sql, (publisher.publisher_id, publisher.publisher_name))
            self.connection.commit()
            return len(publishers)
        except mysql.connector.Error as e:
            print("Veri eklenirken bir hata oluştu:", e)
            return 0
        

    def insertAuthor_of(self,author_ofs):
        try: 
            cursor = self.connection.cursor()
            sql = "INSERT INTO author_of (isbn, author_id) VALUES (%s, %s)"
            for author_of in author_ofs:
                cursor.execute(sql, (author_of.isbn, author_of.author_id))
            self.connection.commit()
            return len(author_ofs)
        except mysql.connector.Error as e:
            print("Veri eklenirken bir hata oluştu:", e)
            return 0
        

    def functionQ1(self):
        try:
            cursor = self.connection.cursor()
            sql = "SELECT B.isbn, B.first_publish_year, B.page_count, P.publisher_name FROM book B, publisher P WHERE B.publisher_id = P.publisher_id AND B.page_count = (SELECT MAX(page_count) FROM book) ORDER BY B.isbn ASC"
            cursor.execute(sql)
            results = cursor.fetchall()
            return [QueryResult.ResultQ1(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ2(self,author_id1, author_id2):
        try:
            cursor = self.connection.cursor()
            sql = "select publisher_id, AVG(page_count) from BOOK WHERE publisher_id IN (SELECT publisher_id FROM BOOK WHERE isbn IN (SELECT isbn FROM author_of WHERE author_id = %s INTERSECT SELECT isbn FROM author_of WHERE author_id = %s )) GROUP BY publisher_id ORDER BY publisher_id ASC"
            cursor.execute(sql, (author_id1, author_id2))
            results = cursor.fetchall()
            return [QueryResult.ResultQ2(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ3(self,author_name):
        try:
            cursor = self.connection.cursor()
            sql = "SELECT B.book_name, B.category, B.first_publish_year FROM book B, author A, author_of AO WHERE A.author_name = %s AND A.author_id = AO.author_id AND AO.isbn = B.isbn ORDER BY B.first_publish_year ASC LIMIT 1"
            cursor.execute(sql, (author_name,))
            results = cursor.fetchall()
            return [QueryResult.ResultQ3(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ4(self):
        try:
            cursor = self.connection.cursor()
            sql = "SELECT publisher_id, category from Book WHERE  publisher_id IN (SELECT publisher_id FROM BOOK WHERE publisher_id IN (SELECT publisher_id FROM publisher WHERE LENGTH(publisher_name) - LENGTH(REPLACE(publisher_name, ' ', '')) >= 2 ) group by publisher_id HAVING AVG(rating)>3) GROUP BY publisher_id, category HAVING SUM(isbn) >= 3 ORDER BY publisher_id ASC, category ASC "
            cursor.execute(sql)
            results = cursor.fetchall()
            return [QueryResult.ResultQ4(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ5(self,author_id):
        try:
            cursor = self.connection.cursor()
            sql =""" SELECT DISTINCT a.author_id, a.author_name
                    FROM author a
                    INNER JOIN author_of ao ON a.author_id = ao.author_id
                    INNER JOIN book b ON ao.isbn = b.isbn
                    INNER JOIN publisher p ON b.publisher_id = p.publisher_id
                    WHERE p.publisher_id IN (
                        SELECT publisher_id
                        FROM author_of
                        INNER JOIN book ON author_of.isbn = book.isbn
                        WHERE author_id = %s
                    )
                    ORDER BY a.author_id ASC"""
            cursor.execute(sql, (author_id,))
            results = cursor.fetchall()
            return [QueryResult.ResultQ5(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ6(self):
        try:
            cursor = self.connection.cursor()
            sql ="""SELECT author_id,isbn FROM 
                    author_of WHERE 
                    author_id IN(
                    SELECT ao.author_id
                    FROM author_of ao
                    INNER JOIN book b ON ao.isbn = b.isbn 
                    WHERE b.publisher_id IN (
                        SELECT b.publisher_id 
                        FROM author_of ao
                        INNER JOIN book b ON ao.isbn = b.isbn
                        GROUP BY b.publisher_id
                        HAVING COUNT(DISTINCT ao.author_id) = 1
                    )
                    GROUP BY ao.author_id 
                    HAVING COUNT(b.publisher_id) != 1
                    )
                    GROUP BY author_id, isbn
                    ORDER BY author_id, isbn ASC"""
            cursor.execute(sql)
            results = cursor.fetchall()
            return [QueryResult.ResultQ6(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
            
        
    def functionQ7(self,rating):
        try:
            cursor = self.connection.cursor()
            sql = "SELECT publisher_id, publisher_name FROM publisher WHERE publisher_id IN (SELECT publisher_id FROM book WHERE category = 'Roman' GROUP BY publisher_id HAVING COUNT(isbn) >= 2 AND AVG(rating) > %s) ORDER BY publisher_id ASC"
            cursor.execute(sql, (rating,))
            results = cursor.fetchall()
            return [QueryResult.ResultQ7(*row) for row in results]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ8(self):
        try:
            cursor = self.connection.cursor()
            insert_sql = """
                    INSERT INTO phw1 (isbn, book_name, rating)
                    SELECT b.isbn, b.book_name, b.rating
                    FROM book b
                    INNER JOIN (
                        SELECT book_name, MIN(rating) AS min_rating
                        FROM book
                        GROUP BY book_name
                        HAVING COUNT(DISTINCT isbn) > 1
                    ) AS subquery ON b.book_name = subquery.book_name AND b.rating = subquery.min_rating;
            """
            cursor.execute(insert_sql)
            select_sql = """SELECT isbn, book_name, rating FROM phw1 ORDER BY isbn ASC"""
            cursor.execute(select_sql)
            results = cursor.fetchall()
            return [QueryResult.ResultQ8(*row) for row in results]

        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return []
        

    def functionQ9(self,keyword):
        try:
            cursor = self.connection.cursor()
            sql = "UPDATE book SET rating = LEAST(5, rating + 1) WHERE book_name LIKE %s AND rating < 4"
            cursor.execute(sql, (f'%{keyword}%',))
            self.connection.commit()
            cursor.execute("SELECT SUM(rating) FROM book")
            results = cursor.fetchone()
            return results[0]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return -1
        

    def function10(self):
        try:
            cursor = self.connection.cursor()
            delete_sql = """
            DELETE FROM publisher
            WHERE publisher_id NOT IN (
                SELECT DISTINCT publisher_id
                FROM book
                )
                """
            cursor.execute(delete_sql)
            
            select_sql = """SELECT COUNT(*) FROM publisher"""
            cursor.execute(select_sql)
            results = cursor.fetchone()
            return results[0]
        except mysql.connector.Error as e:
            print("Sorgu calistirilirken bir hata oluştu:", e)
            return -1
