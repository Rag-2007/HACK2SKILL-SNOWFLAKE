import snowflake.connector

def create_tables():
    conn = snowflake.connector.connect(
    user="raghu07",
    password="Raghuveer11020072007",
    account="atcexwf-hack2skill",
    warehouse="COMPUTE_WH",
    database="INSPECTION_DB",
    schema="PUBLIC"
    )

    curs = conn.cursor()

    curs.execute("CREATE DATABASE IF NOT EXISTS INSPECTION_DB")
    curs.execute("USE DATABASE INSPECTION_DB")
    curs.execute("USE SCHEMA PUBLIC")
    curs.execute("""
    CREATE TABLE IF NOT EXISTS IMAGE_TABLE (
        image_id   INTEGER IDENTITY(1,1),
        image_path STRING NOT NULL,
        notes      STRING,
        PRIMARY KEY (image_id)
    );
    """)

    curs.execute("""
    CREATE TABLE IF NOT EXISTS INSPECTION_TABLE (
        inspection_id   INTEGER IDENTITY(1,1),
        property_id     STRING NOT NULL,
        room            STRING NOT NULL,
        image_id        INTEGER NOT NULL,
        inspection_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (inspection_id),
        FOREIGN KEY (image_id) REFERENCES IMAGE_TABLE(image_id)
    );
    """)

    curs.execute("""
    CREATE TABLE IF NOT EXISTS RSIK_TABLE (
        property_id STRING  NOT NULL ,
        risk_score INTEGER ,
        summary STRING ,
        PRIMARY KEY(property_id)
    );
    """)
    conn.commit() 
    conn.close()



def insert_image(path, notes):
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )

    curs = conn.cursor()

    curs.execute("""
        INSERT INTO IMAGE_TABLE (image_path, notes)
        VALUES (%s, %s)
    """, (path, notes))

    conn.commit()
    conn.close()

    
    
def insert_inspection(pid, rname):
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )

    curs = conn.cursor()

    curs.execute("SELECT MAX(image_id) FROM IMAGE_TABLE")
    iid = curs.fetchone()[0]

    curs.execute("""
        INSERT INTO INSPECTION_TABLE (property_id, room, image_id)
        VALUES (%s, %s, %s)
    """, (pid, rname, iid))

    conn.commit()
    conn.close()


def insert_risk(pid,score,summary) :
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )

    curs = conn.cursor()
    curs.execute(
        "DELETE FROM RSIK_TABLE WHERE property_id = %s",
        (pid,)
    )
    curs.execute("INSERT INTO RSIK_TABLE  VALUES (%s,%s,%s)",(pid,score,summary)) 
    conn.commit()
    conn.close()
    

def create_risk_procedure():
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()

    curs.execute("""
        CREATE OR REPLACE PROCEDURE CALCULATE_PROPERTY_RISK_AI(pid STRING)
        RETURNS INT
        LANGUAGE SQL
        AS
        $$
        DECLARE
            total_risk INT DEFAULT 0;
        BEGIN
            SELECT
                SUM(
                    CASE
                        WHEN LOWER(im.notes) LIKE '%crack%' THEN 30
                        WHEN LOWER(im.notes) LIKE '%damp%' OR LOWER(im.notes) LIKE '%leak%' THEN 40
                        WHEN LOWER(im.notes) LIKE '%wiring%' THEN 50
                        WHEN LOWER(im.notes) LIKE '%mold%' THEN 60
                        ELSE 0
                    END
                )
            INTO :total_risk
            FROM INSPECTION_TABLE i
            JOIN IMAGE_TABLE im
                ON i.image_id = im.image_id
            WHERE i.property_id = :pid;

            RETURN COALESCE(total_risk, 0);
        END;
        $$;
    """)

    conn.close()


def calculate_risk_ai(property_id):
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()

    curs.execute(
        "CALL CALCULATE_PROPERTY_RISK_AI(%s)",
        (property_id,)
    )

    risk = curs.fetchone()[0]
    conn.close()
    return risk

def get_defect_notes(property_id):
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()

    curs.execute("""
        SELECT LISTAGG(im.notes, '; ')
        FROM INSPECTION_TABLE i
        JOIN IMAGE_TABLE im
            ON i.image_id = im.image_id
        WHERE i.property_id = %s
    """, (property_id,))

    notes = curs.fetchone()[0] or "No specific defects recorded."
    conn.close()
    return notes

def build_ai_note(property_id, risk, defect_notes):
    if risk == 0:
        severity = "low"
        instruction = "No major structural or safety risks were detected."
    elif risk < 50:
        severity = "moderate"
        instruction = "Some structural or safety concerns were identified."
    else:
        severity = "high"
        instruction = "Serious safety and structural risks were identified."

    note = f"""
    You are an expert building safety inspector.

    Property ID: {property_id}
    Overall Risk Level: {severity}
    Risk Score: {risk}/100

    Inspection Findings:
    {defect_notes}

    Guidance:
    {instruction}

    Generate a clear, non-technical inspection summary for a home buyer.
    Highlight safety concerns and suggest whether immediate action is required.
    """

    return note.strip()

def create_note_for_ai(property_id,risk):
    defect_notes = get_defect_notes(property_id)

    ai_note = build_ai_note(
        property_id=property_id,
        risk=risk,
        defect_notes=defect_notes
    )

    return ai_note

def ai_summary_generate(pid) :
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()
    
    risk = calculate_risk_ai(pid)
    ai_notes = create_note_for_ai(pid,risk)
    curs.execute("""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'claude-3-5-sonnet',
            %s
        )
    """, (ai_notes,))
    ai_summary = curs.fetchone()[0]
    conn.close()
    insert_risk(pid,risk,ai_summary)


def select_pids() :
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()  
    
    curs.execute("""
        SELECT DISTINCT(property_id) FROM INSPECTION_TABLE ;
    """) 
    lst = [row[0] for row in curs.fetchall()]
    return lst  

def select_pids_risk():
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()  
    
    curs.execute("""
        SELECT DISTINCT(property_id) FROM  RSIK_TABLE;
    """) 
    lst = [row[0] for row in curs.fetchall()]
    return lst 

def get_risk_summary(pid):
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()

    curs.execute("""
        SELECT risk_score, summary
        FROM RSIK_TABLE
        WHERE property_id = %s
    """, (pid,))

    result = curs.fetchone()
    conn.close()
    return result   

def get_images(pid):
    conn = snowflake.connector.connect(
        user="raghu07",
        password="Raghuveer11020072007",
        account="atcexwf-hack2skill",
        warehouse="COMPUTE_WH",
        database="INSPECTION_DB",
        schema="PUBLIC"
    )
    curs = conn.cursor()

    curs.execute("""
        SELECT im.image_path
        FROM INSPECTION_TABLE i
        JOIN IMAGE_TABLE im
            ON i.image_id = im.image_id
        WHERE i.property_id = %s
    """, (pid,))

    images = [row[0] for row in curs.fetchall()]
    conn.close()
    return images


if __name__ == "__main__":
    create_tables()
    create_risk_procedure()



