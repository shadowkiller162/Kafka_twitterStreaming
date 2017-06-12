import nexmo

def sms(finalMessage):
    client = nexmo.Client(key='yourKey', secret='yourSecret')

    response = client.send_message({'from': 'yourID',
                                    'to': 'phoneNumber',
                                    'text': finalMessage})

    response = response['messages'][0]

    if response['status'] == '0':
      print('Sent message' + response['message-id'])
      print('Remaining balance is' + response['remaining-balance'])
    else:
      print('Error:' + response['error-text'])
