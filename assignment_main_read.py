import os
import sys
#sys.path.insert(0,"/c/Users/swati/airflow-docker/assignment_main"))
import pandas as pd
import json
import math

class Input1():
      global ls
      
      def __init__(self,post_id,shares):
            self.post_id=post_id
            self.shares=shares

      def set_vals(self,post_id,shares):
            ob=Input1(post_id,shares)
            ls.append(ob)

      def get_vals(self,ob):
            if ob.post_id != '':
                  return ob.post_id,ob.shares
                  #print('post_id: ' , ob.post_id, ' shares: ' , ob.shares)
            #print()
            #print('\n')

      def search(self,post_id):
            #print('comes here', post_id)
            
            for i in range(ls.__len__()):
                  if(ls[i].post_id == post_id):
                        #print(ls[i].post_id, ls[i].shares)
                        return ls[i].shares  
class Input2():
      global ls2
      
      def __init__(self,postid,comments,likes):
            self.postid=postid
            self.comments=comments
            self.likes=likes

      def set_vals2(self,postid,comments,likes):
            ob2=Input2(postid,comments,likes)
            ls2.append(ob2)

      def get_vals2(self,ob2):
            return ob2.postid,ob2.comments,ob2.likes
            #if ob2.postid != '':
            #print('post_id: ' , ob2.postid ,'comments: ' ,\
              #          ob2.comments, ' likes: ' , ob2.likes)
            #print()
            #print('\n')

      def search2(self, postid):
            for i in range(ls2.__len__()):
                  if self.postid==postid:
                        return  i      
def call_script(local_dir,json_input_file,csv_input_file):            
      global ls,ls2,post_id,shares,date,comments,likes ,postid
      global csv_output_file,file2,opfile
      ls=[]
      ls2=[]
      file2=''
      opfile=''
      obj = Input1('', 0)
      obj2 = Input2('', 0,0)
      #json_input_file='data_20210124.json'
      #csv_input_file='engagement_20210124.csv'


      inputfile1=local_dir ##+ '/input_source_1/' data_20210124.json'
      inputfile2=local_dir ##+ '/input_source_2/' #engagement_20210124.csv'
      

      start_pos=json_input_file.find('data_')
      end_pos=json_input_file.find('.json')
      date=json_input_file[start_pos+5:end_pos]

      print('date: ' + date)

      outputfile=local_dir #+ '/output_' + date + '.csv'
      csv_output_file='/output_' + date + '.csv'
      op_df=pd.DataFrame(columns = ['Date', 'Post_Id', 'Shares_Count','Comments_Count'\
                                    ,'Likes_Count'])

      tweets = []
      keys=['post_id','count']

      postids=[]
      post_counts=[]

      post_id=''
      shares=0

#      file_path = os.path.join(inputfile1, 'data_20210124.json')
      arr = os.listdir(inputfile1)

      for x in arr:          
          if x==json_input_file:
              file_path = os.path.join(inputfile1, x)
              #print(file_path)
              for line in open(file_path, 'r'):
                    tweets.append(json.loads(line))




      for item in tweets:
            for key,val in item.items():

                  if key in keys:
                        #print(item[key])
                        post_id=item[key]
                        
                        #print(type(item[key]))
                  #print(type(val))
                  else:
                        if isinstance(val,dict):
                              for k,v in val.items():
                                    if k in keys:
                                          #print(val[k])
                                          shares=val[k]

                  #obj = Input1.set_vals(post_id,shares)
            obj.set_vals(post_id,shares)
            post_id=''
            shares=0



      for i in range(ls.__len__()):
            obj.get_vals(ls[i])                             

      ##reading second file
      arr = os.listdir(inputfile2)

      for x in arr:          
          if x==csv_input_file:
              file2 = os.path.join(inputfile2, x)
              #print(file2)
      df2=pd.read_csv(file2,header=0,sep=',',index_col=0)


      for index, row in df2.iterrows():
            postid=index
            #print(len(row.tolist()))

            if math.isnan(row[0]):
                  comments=0
            else:
                  comments=row[0]

            if math.isnan(row[1]):
                  likes=0
            else:
                  likes=row[1]

            obj2.set_vals2(postid,comments,likes)
                        
      for i in range(ls2.__len__()):
            postid,comments,likes=obj2.get_vals2(ls2[i])
            #print(postid)
            shares=obj.search(postid)
            #print(postid,shares,comments,likes)

            op_df = op_df.append({'Date' : date, 'Post_Id' : postid, 'Shares_Count' : shares\
                            ,'Comments_Count':comments,'Likes_Count':likes},\
                           ignore_index = True)

      arr = os.listdir(outputfile)
      #print('comes here , outputfile ' + outputfile)
      opfile=outputfile+csv_output_file
      op_df.to_csv(opfile,index=False)






