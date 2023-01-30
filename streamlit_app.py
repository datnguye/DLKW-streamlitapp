import streamlit as st
import snowflake.connector
import pandas

st.title("Data Lake Workshop - Hands on")

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()
      
@st.experimental_memo(ttl=600)
def run_query_scalar(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()
    
    
@st.experimental_memo(ttl=600)
def execute_no_query(query):
    with conn.cursor() as cur:
        cur.execute(query)

      
# run a snowflake query and put it all in a var called my_catalog
my_catalog = run_query("select color_or_style from catalog_for_website")

# put the dafta into a dataframe
df = pandas.DataFrame(my_catalog)

# temp write the dataframe to the page so I Can see what I am working with
# st.write(df)
# put the first column into a list
color_list = df[0].values.tolist()
# print(color_list)

# Let's put a pick list here so they can pick the color
option = st.selectbox('Pick a sweatsuit color or style:', list(color_list))

# We'll build the image caption now, since we can
product_caption = 'Our warm, comfortable, ' + option + ' sweatsuit!'

# use the option selected to go back and get all the info from the database
df2 = run_query_scalar("select direct_url, price, size_list, upsell_product_desc from catalog_for_website where color_or_style = '" + option + "';")
st.image(df2[0], width=400, caption= product_caption)
st.write('Price: ', df2[1])
st.write('Sizes Available: ',df2[2])
st.write(df2[3])

# Let's create form for complaination
def submit_complain(name, email, content):
    if email and content: 
        # record into snowflake
        execute_no_query("""
            create table if not exists customer_complaination (name varchar, email varchar, content varchar)
        """)
        execute_no_query(f"""
            insert into customer_complaination values ('{name}','{email}','{content}')
        """)
        st.success("Your feedback has been sent! ✅")
    
with st.form(key="form_complaination"):
    st.write("Feed us your apple")
    c1, c2 = st.columns(2)
    
    with c1:
        ti_name = st.text_input("Name")
    with c2:
        ti_email = st.text_input("Email")
    tae_complaination = st.text_area(label="What would you like us to improve?")
    
    submit = st.form_submit_button(
        label="Complains",
        on_click=submit_complain,
        kwargs=dict(
            name=ti_name, 
            email=ti_email, 
            content=tae_complaination
        )
    )
    
if submit:
    if not tae_complaination:
        st.error("Complaination is required")
        st.stop()
    if not ti_email:
        st.error("Email is required")
        st.stop()
