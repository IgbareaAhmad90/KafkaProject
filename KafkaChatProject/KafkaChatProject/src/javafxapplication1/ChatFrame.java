/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javafxapplication1;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 *
 * @author Igbar
 */
public class ChatFrame extends javax.swing.JFrame {

	private static final DateFormat sdf = new SimpleDateFormat("HH:mm:ss");
	private static final long serialVersionUID = 1L;
	public static String UserName;
    public Kafka kafka =null;
    /** 
     * Creates new form ChatFrame
     */
    public ChatFrame(String UserName) {
        ChatFrame.UserName=UserName;
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    private void initComponents() {

        ListViewMessages = new java.awt.List();
        ListViewTopics = new java.awt.List();
        disconnectionButton = new javax.swing.JButton();
        connectionButton1 = new javax.swing.JButton();
        RegisterButton = new javax.swing.JButton();
        LeaveButton = new javax.swing.JButton();
        TopicField = new javax.swing.JTextField();
        SendMessageField = new javax.swing.JTextField();
        SendTopicField = new javax.swing.JTextField();
        SendMessageButton = new javax.swing.JButton();
        Help = new javax.swing.JLabel();
        jLabel1 = new javax.swing.JLabel();
        jLabel2 = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        ListViewMessages.setFont(new java.awt.Font("Dialog", 0, 18)); // NOI18N
        ListViewMessages.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                ListViewMessagesActionPerformed(evt);
            }
        });

        ListViewTopics.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                ListViewTopicsActionPerformed(evt);
            }
        });

        disconnectionButton.setBackground(new java.awt.Color(255, 51, 51));
        disconnectionButton.setText("Disconnection");
        disconnectionButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                disconnectionButtonActionPerformed(evt);
            }
        });

        connectionButton1.setBackground(new java.awt.Color(0, 153, 0));
        connectionButton1.setText("Connection");
        connectionButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                try {
					connectionButton1ActionPerformed(evt);
				} catch (InterruptedException e) {
					return;
				}
            }
        });

        RegisterButton.setBackground(new java.awt.Color(0, 153, 0));
        RegisterButton.setText("Register");
        RegisterButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                RegisterButtonActionPerformed(evt);
            }
        });

        LeaveButton.setBackground(new java.awt.Color(255, 51, 51));
        LeaveButton.setText("Leave ");
        LeaveButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                LeaveButtonActionPerformed(evt);
            }
        });

        TopicField.setToolTipText("write here The Topic To Add/Remove");
       

        SendMessageField.setToolTipText("write here The Message To Send");
       

        SendTopicField.setToolTipText("write here The Topic To Send Or Select By List");
     

        SendMessageButton.setBackground(new java.awt.Color(0, 153, 0));
        SendMessageButton.setText("Send");
        SendMessageButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                try {
					SendMessageButtonActionPerformed(evt);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        });

        Help.setText("You Can To See The Return Messages Here .. I am here to Help you :)");

        jLabel1.setFont(new java.awt.Font("Tahoma", 0, 10)); // NOI18N
        jLabel1.setText("ListView Displays all messages that follow Topics chosen");

        jLabel2.setFont(new java.awt.Font("Tahoma", 0, 10)); // NOI18N
        jLabel2.setText("List Topics");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(layout.createSequentialGroup()
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(SendTopicField, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 104, Short.MAX_VALUE)
                            .addGroup(layout.createSequentialGroup()
                                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                                        .addComponent(TopicField)
                                        .addComponent(ListViewTopics, javax.swing.GroupLayout.DEFAULT_SIZE, 102, Short.MAX_VALUE))
                                    .addComponent(jLabel2))
                                .addGap(0, 2, Short.MAX_VALUE)))
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(layout.createSequentialGroup()
                                .addComponent(SendMessageField, javax.swing.GroupLayout.PREFERRED_SIZE, 379, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(SendMessageButton, javax.swing.GroupLayout.PREFERRED_SIZE, 91, javax.swing.GroupLayout.PREFERRED_SIZE))
                            .addGroup(layout.createSequentialGroup()
                                .addComponent(RegisterButton, javax.swing.GroupLayout.PREFERRED_SIZE, 100, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(LeaveButton, javax.swing.GroupLayout.PREFERRED_SIZE, 110, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(connectionButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 135, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(disconnectionButton))
                            .addComponent(ListViewMessages, javax.swing.GroupLayout.PREFERRED_SIZE, 478, javax.swing.GroupLayout.PREFERRED_SIZE)
                            .addComponent(jLabel1))
                        .addContainerGap(33, Short.MAX_VALUE))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(Help)
                        .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(8, 8, 8)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(disconnectionButton)
                    .addComponent(connectionButton1)
                    .addComponent(RegisterButton)
                    .addComponent(LeaveButton)
                    .addComponent(TopicField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel1)
                    .addComponent(jLabel2))
                .addGap(3, 3, 3)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(ListViewMessages, javax.swing.GroupLayout.PREFERRED_SIZE, 350, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(ListViewTopics, javax.swing.GroupLayout.PREFERRED_SIZE, 350, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(SendMessageField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(SendTopicField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(SendMessageButton))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(Help)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    @SuppressWarnings("deprecation")
	private void connectionButton1ActionPerformed(java.awt.event.ActionEvent evt) throws InterruptedException {
    	if(kafka != null && kafka.status == true) {
			PrintMessage("You Are connect");
			return ;
		}
    	// Create New Object kafka 
    	 this.kafka = new Kafka(UserName);
    	 if(kafka.status == false) {
    		 PrintMessage("Error - Check your Connection with the Server");
    		 return;
    	 }
    	 
    	 // extra 
    	 // if the client connected to Server after disconnected subscribe the topics again
    	 if(ListViewTopics.countItems() !=0) {
    		 for(int i = 0 ; i<ListViewTopics.countItems();i++) {
    			 String Topic = ListViewTopics.getItem(i);
    	    	 kafka.SubscribeTopic(Topic);
    		 }
    	 }
         // Add The Topic To List View 
         this.kafka.ConsumerForword();
         if(kafka.TopicIsExistByZookeeper("BrodCast")) {
    		 PrintMessage("Connection sccssesful");
         }else {
    		 PrintMessage("Error - Check your Connection with the Server");
         }
        	 
		 
    }

    private void disconnectionButtonActionPerformed(java.awt.event.ActionEvent evt) {
    	// Button Disconnection
    	if(kafka == null || kafka.status == false){
			PrintMessage("You Are disconnect");
			return;
		}
    	
    	kafka.Close();
		PrintMessage("disonnection sccssesful");
        System.out.println("disconnection");
    }//GEN-LAST:event_disconnectionButtonActionPerformed

    private void RegisterButtonActionPerformed(java.awt.event.ActionEvent evt) {
    	// Button Register Topic 
   
    	//Check connection
    	if(kafka == null || kafka.status == false){
			PrintMessage("You Are disconnect"); 
			return;
		}
    	// get Topic
        String Topic = TopicField.getText();
        if(Topic.isEmpty()){
              PrintMessage("Enter TopicName to Field Topic   ");
              return ;
        }        
        TopicField.setText("");
        // Check if the Topic Is Exist By Zookeeper ListTopics() 
        boolean topicExistByZookeeper = kafka.TopicIsExistByZookeeper(Topic);
        if(topicExistByZookeeper == false ){
        	 kafka.CreateTopic(Topic);
            PrintMessage("create a New Topic ("+Topic+")");
            //ToDo - Print to  LogFile 
        }
        
        	// the topic is exist by zookeeper 
        	// check if Subscribe the Topic 
        	if(kafka.TopicIsSubscribe(Topic)){
        		// topic is subscribe - true 
                PrintMessage(" this Topic("+Topic+") is Exist");
        	}else {
        		//Subscribe the Topic 
        		 // Add The Topic To List View 
                kafka.SubscribeTopic(Topic);
                ListViewTopics.add(Topic);
                PrintMessage("New subscribe by this Topic("+Topic+")");
        	}	
      }
       
    private void LeaveButtonActionPerformed(java.awt.event.ActionEvent evt) {
    	// Leave Topic Button
        System.out.println("Leave Topic");
    	//Check connection
    	if(kafka == null || kafka.status == false){
			PrintMessage("You Are disconnect");
			return;
		}
        String Topic = TopicField.getText();
        if(Topic.isEmpty()){
              PrintMessage("Enter TopicName to Field Topic");
              return ;
        }
        TopicField.setText("");

        // Check if The Topic is Exist 
        boolean TopicExist = kafka.TopicIsExistByZookeeper(Topic);
        if(TopicExist == false ){
            PrintMessage("Topic ("+Topic+") is Not Exist By Zookeeper");
            return;
            //ToDo - Print to  LogFile 
        }
        
        if(kafka.TopicIsSubscribe(Topic)) {
            // Remove Topic on ListView 
        	  ListViewTopics.remove(Topic);
              kafka.UnsubscribeTopic(Topic);
              PrintMessage("Remove Topic not subscribe this Topic("+Topic+")");
        }else {
            PrintMessage("you are not subscribe this Topic("+Topic+")");
        }
          //kafka.removeTopic();
    }

    private void SendMessageButtonActionPerformed(java.awt.event.ActionEvent evt) throws InterruptedException {//GEN-FIRST:event_SendMessageButtonActionPerformed
       // Send Message Button 
    	//Check connection
    	if(kafka == null || kafka.status == false){
			PrintMessage("You Are disconnect !!");
			return;
		}
    	//Get Message -> set Result -> var Message
        String Message = SendMessageField.getText();
        if(Message.isEmpty()){
              PrintMessage("Enter Message to Field Message");
              return ;
        }
    	//Get Topic -> set Result -> var Topic
        String Topic = SendTopicField.getText();
        if(Topic.isEmpty()){
              PrintMessage("Enter Topic to Field Topic Or Select the Topic RightList");
              return ;
        }
        // Check if the Topic is exist by ServerKafka
        boolean topicExistByZookeeper = kafka.TopicIsExistByZookeeper(Topic);
        if(topicExistByZookeeper == false ){
        	this.kafka.CreateTopic(Topic);
            PrintMessage("create a New Topic ("+Topic+")");
            //ToDo - Print to  LogFile 
        }
        	// the topic is exist by zookeeper 
        	// check if Subscribe the Topic 
        if(!this.kafka.TopicIsSubscribe(Topic)){
        		//Subscribe the Topic 
        		 // Add The Topic To List View 
                kafka.SubscribeTopic(Topic);
                ListViewTopics.add(Topic);
                PrintMessage("subsicribe New Topic ("+Topic+") and Send the Message" 
                        +"\nSend Message("+Message+") To this Topic("+Topic+")");
        }else {	
        	PrintMessage("Send Message("+Message+") To this Topic("+Topic+")");
        }
        Thread.sleep(100);	
        String time = sdf.format(new java.util.Date());
        String MsgByFormat = String.format("(%s) %s %s - %s", Topic,UserName,time,Message);
        this.kafka.SendMessage(Topic,MsgByFormat);
        //ListViewMessages.add(MsgByFormat);
        SendMessageField.setText("");  
    }

    private void ListViewMessagesActionPerformed(java.awt.event.ActionEvent evt) {// TODO add your handling code here:
    }

    private void ListViewTopicsActionPerformed(java.awt.event.ActionEvent evt) {
    	// List View Topics 
        String TopicSelect = evt.getActionCommand();
        SendTopicField.setText(TopicSelect);  
        TopicField.setText(TopicSelect);  
    }

    /////////////////////////////////////////////////////////////////////
    static void PrintMessage(String Message){
        System.out.println(Message);
        TopicFile.log(UserName,Message);
        Help.setText(Message);
        return;
    }
    
    static void PrintMessageToList(String Message){
    	ListViewMessages.add(Message);
        return;
    }
    /////////////////////////////////////////////////////////////////////
    
    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(ChatFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(ChatFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(ChatFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(ChatFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

       
    }

    
    // Variables declaration - do not modify//GEN-BEGIN:variables
    private static javax.swing.JLabel Help;
    private javax.swing.JButton LeaveButton;
    private static java.awt.List ListViewMessages;
    private java.awt.List ListViewTopics;
    private javax.swing.JButton RegisterButton;
    private javax.swing.JButton SendMessageButton;
    private javax.swing.JTextField SendMessageField;
    private javax.swing.JTextField SendTopicField;
    private javax.swing.JTextField TopicField;
    private javax.swing.JButton connectionButton1;
    private javax.swing.JButton disconnectionButton;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    // End of variables declaration//GEN-END:variables
}
