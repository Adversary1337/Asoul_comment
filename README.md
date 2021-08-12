# 1. 数据库表结构

1. bilibili.dynamic表项

   ![image-20210812220753186](.\README.assets\image-20210812220753186.png)

2. bilibili.user_comment表项

![image-20210812220832655](.\README.assets\image-20210812220832655.png)

# 2. 如何运行

1. pip3 install bilibili_api

2. 找到lib/site-packages/bilibili_api/comment.py

   下的get_comments函数，改成如下格式

   ```python
   async def get_comments(oid: int,
                          type_: int,
                          page_index: int = 1,
                          order: OrderType = OrderType.TIME,
                          credential: Credential = None):
       """
       获取资源评论列表。
   
       Args:
           oid        (int)                 : 资源 ID。
           type_      (int)        : 资源int。
           page_index (int, optional)       : 页码. Defaults to 1.
           order      (OrderType, optional) : 排序方式枚举. Defaults to OrderType.TIME.
           credential (Credential, optional): 凭据。Defaults to None.
   
       Returns:
           dict: 调用接口返回的内容。
       """
       if page_index <= 0:
           raise ArgsException("page_index 必须大于或等于 1")
   
       api = API["comment"]["get"]
       params = {
           "pn": page_index,
           "type": type_,
           "oid": oid,
           "sort": order.value
       }
       return await request("GET", api["url"], params=params, credential=credential)
   
   ```

   

# 3. 注意事项

1. 为了防止cr的大手，每个get_comments后面均设置了延时，目前没摸清套路，只知道每秒获取1条评论的频率一定不会被封。。。如果发现返回值412，可以通过solve_412函数进行操作（添加代理、更换ip等等，但是目前国内代理均需实名认证，所以我使用休息半小时代替）
2. 部分动态的评论需要rid进行爬取，所以设置了is_rid标志位进行标注，在数据库中表现为type字段为负数（此时应使用rid）

# 4. TODO

1. 动态添加latest_comment_timestamp字段，避免重复爬取，同时可以更新早期动态的新鲜评论