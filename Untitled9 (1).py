#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
orders = pd.read_csv('orders.csv')
print(orders.head())


# In[2]:


users = pd.read_json('users.json')
print(users.head())


# In[3]:


import sqlite3
conn = sqlite3.connect(':memory:')
with open('restaurants.sql', 'r') as file:
    sql_script = file.read()
    conn.executescript(sql_script)
restaurants = pd.read_sql_query("SELECT * FROM restaurants", conn)
print(restaurants.head())


# In[4]:


orders_users = pd.merge(
    orders,
    users,
    how='left',
    left_on='user_id',
    right_on='user_id'
)
final_df = pd.merge(
    orders_users,
    restaurants,
    how='left',
    left_on='restaurant_id',
    right_on='restaurant_id'
)
print(final_df.head())


# In[5]:


final_df.to_csv('final_food_delivery_dataset.csv', index=False)

print("Final dataset saved as 'final_food_delivery_dataset.csv'")


# In[12]:


print(final_df[final_df['membership']=='Gold'].groupby('city')
      ['total_amount'].sum().idxmax())


# In[28]:


# Drop rows with missing values in order_amount or cuisine
df_clean = final_df.dropna(subset=['total_amount', 'cuisine'])

# Cuisine with highest average order value
top_cuisine = df_clean.groupby('cuisine')['total_amount'].mean().idxmax()
print("Cuisine with highest average order value:", top_cuisine)


# In[30]:


user_total = final_df.groupby('user_id')['total_amount'].sum()
num_users = (user_total > 1000).sum()
print("Number of users with total orders > ₹1000:", num_users)


# In[31]:


df_clean = final_df.dropna(subset=['rating', 'total_amount'])

rating_revenue = df_clean.groupby('rating')['total_amount'].sum()

top_rating = rating_revenue.idxmax()
print("Restaurant rating with highest total revenue:", top_rating)


# In[32]:


gold = final_df[final_df['membership']=='Gold'].dropna(subset=['total_amount', 'city'])
top_city = gold.groupby('city')['total_amount'].mean().idxmax()
print("City with highest average order value among Gold members:", top_city)


# In[34]:


top_cuisine = (final_df.groupby('cuisine')
               .agg({'restaurant_id':'nunique','total_amount':'sum'})
               .assign(revenue_per_rest=lambda x: x['total_amount']/x['restaurant_id'])
               .sort_values('revenue_per_rest', ascending=False)
               .index[0])
print("Top cuisine:", top_cuisine)


# In[35]:


total_orders = len(final_df)
gold_orders = len(final_df[final_df['membership']=='Gold'])
percent_gold = round((gold_orders / total_orders) * 100)
print("Percentage of orders by Gold members:", percent_gold, "%")


# In[36]:


top_restaurant = final_df.groupby('restaurant_id').agg(
    total_orders=('order_id','count'),
    avg_order_value=('total_amount','mean')
).query('total_orders < 20')['avg_order_value'].idxmax()

print("Restaurant with highest average order value (<20 orders):", top_restaurant)


# In[38]:


df_clean = final_df.dropna(subset=['city','cuisine','total_amount'])
combo_revenue = df_clean.groupby(['city','cuisine'])['total_amount'].sum()
top_combo = combo_revenue.idxmax()
print("City & Cuisine combination with highest revenue:", top_combo)


# In[40]:


gold_orders = final_df[final_df['membership']=='Gold']['order_id'].count()
print("Total orders by Gold members:", gold_orders)


# In[41]:


hyderabad_revenue = round(final_df[final_df['city']=='Hyderabad']['total_amount'].sum())
print("Total revenue from Hyderabad:", hyderabad_revenue)


# In[42]:


num_users = final_df['user_id'].nunique()
print("Number of distinct users:", num_users)


# In[43]:


avg_gold = round(final_df[final_df['membership']=='Gold']['total_amount'].mean(), 2)
print("Average order value for Gold members:", avg_gold)


# In[44]:


high_rating_orders = final_df[final_df['rating'] >= 4.5]['order_id'].count()
print("Orders for restaurants with rating >= 4.5:", high_rating_orders)


# In[45]:


gold = final_df[final_df['membership']=='Gold']
top_city = gold.groupby('city')['total_amount'].sum().idxmax()
orders_in_top_city = gold[gold['city']==top_city]['order_id'].count()
print("Orders in top revenue city among Gold members:", orders_in_top_city)


# In[46]:


total_rows = len(final_df)
print("Total number of rows in the final dataset:", total_rows)


# In[ ]:




