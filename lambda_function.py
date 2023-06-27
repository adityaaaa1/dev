import boto3
import pandas as pd
import json
from custom_encoder import CustomEncoder
import logging
print("5")
logger = logging.getLogger()
#print("7")
logger.setLevel(logging.INFO)
#print("9")
dynamodbTableName='Clouddatatable'
#print("51")
dynamodb=boto3.resource('dynamodb')
print("52")
table=dynamodb.Table(dynamodbTableName)
print("53",table)

getMethod='GET'
#print("54")
postMethod='POST'
#print("55")
patchMethod='PATCH'
#print("56")
deleteMethod='DELETE'
#print("57")
healthPath= '/health'
#print("58")
productPath= '/product'
#print("59")
productsPath= '/products'
#print("50")
def lambda_handler(event, context):
    print("20")
    logger.info(event)
    httpMethod=event['httpMethod']
    path=event['path']
    if httpMethod==getMethod and path==healthPath:
        print("25")
        response=buildResponse(200)
        print("27")
    elif httpMethod==getMethod and path==productPath:
        response=getProduct(event['queryStringParameters']['id'])
    elif httpMethod==getMethod and path==productsPath:
        response=getProducts()
    elif httpMethod==postMethod and path==productPath:
        print("46")
        response=saveProduct(json.loads(event['body']))
        print("48")
    elif httpMethod==patchMethod and path==productPath:
        requestBody=json.loads(event['body'])
        response= modifyProduct(requestBody['id'],requestBody['updateKey'],requestBody['updateValue'])
    elif httpMethod==deleteMethod and path==productPath:
        requestBody=json.loads(event['body'])
        response= deleteProduct(requestBody['id'])
    else:
        response=buildResponse(404,'Not Found')
    return response    
    
def getProduct(productId):
    try:
        response=table.get_item(
            Key={
                'id':productId
            }
            )
        print("65",response)    
        if 'Item' in response:
            return  buildResponse(200,response['Item'])
            
        else:
            return buildResponse(404,{'Message':'ProductId, %s is not found' % productId})
    except:
        logger.exception('error occured getProduct')
        
def getProducts():
    try:
        response=table.scan()
        result=response['Items']
        
        while 'LastEvaluatedKey' in response:
            response=table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
            
        body={
            'products':response
        }
        
        return buildResponse(200,body) 
    except:
        logger.exception('error occured getProducts')
        
def saveProduct(requestBody):
    try:
        print("93",requestBody)
        table.put_item(Item=requestBody)
        print("95")
        body={
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        print("100")
        return buildResponse(200,body)
    except:
        logger.exception('no logs saveProduct')

def modifyProduct(productId, updateKey, updateValue):
    try:
        response=table.update_item(
            Key={
                'id':productId
            },
            UpdateExpression='set %s= :value' % updateKey,
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
            )
        body={
                'Operation': 'UPDATE',
                'Message': 'SUCCESS',
                'UpdatedAttributes': response
            }
        return buildResponse(200,body)
            
    except:
        logger.exception('no logs modifyProduct')
        
def deleteProduct(productId):
    try:
        response=table.delete_item(
            Key={
                'id':productId
            },
            )
        ReturnValues='ALL_OLD'
        body={
                'Operation': 'DELETE',
                'Message': 'SUCCESS',
                'deletedItem': response
            }
        return buildResponse(200,body)
    except:
        logger.exception(" no logs deleteProduct")
            
            
    
def buildResponse(statusCode, body=None):
    print("128")
    response={
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    print("136")
    if body is not None:
        response['body']=json.dumps(body, cls=CustomEncoder)
    print("1")    
        
    return response    
    
    
