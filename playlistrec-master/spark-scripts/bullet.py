from pushbullet import Pushbullet
apiKey = 'o.jsMGG789aqsV47OChBCGlPIJjQGli3Qz'
apiKeyRob = "o.uKvCG8jgepkDdoQBoUFzVJ84zCgyQ0VU"

pb = Pushbullet(apiKey)
pbRob = Pushbullet(apiKeyRob)

xperia = pb.get_device('Xperia M2')
oneplus = pbRob.get_device('OnePlus One')

def sendNotificationToMattia(title,body):
    push = xperia.push_note(title, body)
       

def sendNotificationToRoberto(title,body):
    push = oneplus.push_note(title, body)

    




