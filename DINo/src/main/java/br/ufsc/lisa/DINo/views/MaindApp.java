package br.ufsc.lisa.DINo.views;

import java.awt.EventQueue;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import java.awt.BorderLayout;
import javax.swing.JComboBox;
import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.sql.SQLException;
import java.util.List;

import javax.swing.JLabel;
import java.awt.Font;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.ListModel;
import javax.swing.ListSelectionModel;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;

import java.awt.FlowLayout;
import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JTextArea;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JCheckBox;
import java.awt.SystemColor;
import java.awt.Color;

import br.ufsc.lisa.DINo.util.Connector;
import br.ufsc.lisa.DINo.util.MongoConnector;
import br.ufsc.lisa.DINo.util.PostgresDB;
import br.ufsc.lisa.DINo.util.RedisConnector;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.InputMethodListener;
import java.awt.event.InputMethodEvent;
import javax.swing.JScrollBar;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import javax.swing.JFormattedTextField;
import javax.swing.JPasswordField;

public class MaindApp {

	private JFrame frame;
	private JTextField textFieldHost;
	private JTextField textFieldPort;
	private JTextField textFieldUsername;
	private JPasswordField textFieldPassword;
	private JTextField textFieldImport;
	private PostgresDB postgresDb;
	private RedisConnector redisDb;
	private Connector targetDb;
	private JList listDb;
	private String tempUrl;
	private JTextField textFieldPrefixo;
	private JTextField textFieldConsole;
	private JTextField textFieldRedisHost;
	private JTextField textFieldRedisPort;
	private JTextField textFieldRedisPassword;
	private JPanel panelDatabase;
	private JList listColumns;
	private JTabbedPane tabbedPaneTarget;
	private JButton btnGenerateSql;
	private JButton btnGetSelectedColumnsPK;
	private JList listValue; 
	private JList listPk;
	private JButton btnGetSelectedColumnsVALUE;
	private JButton btnImport;
	private JComboBox<Connector> comboBoxTargets;
	private JTextField textFieldTargetUser;
	private JTextField textFieldTargetDbName;

	

	public DefaultListModel<String> listToListModel (List<String> itens){
		DefaultListModel<String> newList = new DefaultListModel<String>();
		itens.forEach(item -> {
			newList.addElement(item);
		});
		return newList;
	}

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MaindApp window = new MaindApp();
					window.frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public MaindApp() throws ClassNotFoundException, SQLException {
		initialize();
		postgresDb = null;
		targetDb = null;
	}

	private void initialize() throws ClassNotFoundException, SQLException {
		frame = new JFrame();
		frame.setBounds(100, 100, 660, 525);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setLocationRelativeTo(null);
		
		panelDatabase = new JPanel();
		listColumns = new JList();
		tabbedPaneTarget = new JTabbedPane(JTabbedPane.TOP);

		JPanel mainPanel = new JPanel();
		mainPanel.addComponentListener(new ComponentAdapter() {
			@Override
			public void componentShown(ComponentEvent arg0) {

			}
		});
		frame.getContentPane().add(mainPanel, BorderLayout.CENTER);
		mainPanel.setLayout(null);

		JLabel lblSource = new JLabel("Source");
		lblSource.setBounds(97, -3, 78, 27);
		lblSource.setFont(new Font("Dialog", Font.BOLD, 14));
		mainPanel.add(lblSource);

		final JTabbedPane tabbedPaneSource = new JTabbedPane(JTabbedPane.TOP);
		tabbedPaneSource.setBounds(12, 22, 313, 323);
		mainPanel.add(tabbedPaneSource);

		JPanel panelServer = new JPanel();
		tabbedPaneSource.addTab("Server", null, panelServer, null);
		panelServer.setLayout(null);

		JLabel lblConnection = new JLabel("Connection Postgres");
		lblConnection.setFont(new Font("Dialog", Font.BOLD, 13));
		lblConnection.setBounds(22, 12, 153, 17);
		panelServer.add(lblConnection);

		JLabel lblHost = new JLabel("Host: ");
		lblHost.setBounds(22, 58, 50, 15);
		panelServer.add(lblHost);

		JLabel lblPort = new JLabel("Port: ");
		lblPort.setBounds(22, 85, 70, 15);
		panelServer.add(lblPort);

		JLabel lblUsername = new JLabel("Username: ");
		lblUsername.setBounds(22, 112, 101, 15);
		panelServer.add(lblUsername);

		JLabel lblPassword = new JLabel("Password: ");
		lblPassword.setBounds(22, 139, 101, 15);
		panelServer.add(lblPassword);

		textFieldHost = new JTextField();
		textFieldHost.setText("localhost");
		textFieldHost.setBounds(102, 56, 163, 19);
		panelServer.add(textFieldHost);
		textFieldHost.setColumns(10);

		textFieldPort = new JTextField();
		textFieldPort.setText("5432");
		textFieldPort.setBounds(102, 83, 163, 19);
		panelServer.add(textFieldPort);
		textFieldPort.setColumns(10);

		textFieldUsername = new JTextField();
		textFieldUsername.setText("postgres");
		textFieldUsername.setBounds(102, 112, 163, 19);
		panelServer.add(textFieldUsername);
		textFieldUsername.setColumns(10);

		textFieldPassword = new JPasswordField();
		textFieldPassword.setBounds(102, 139, 163, 19);
		panelServer.add(textFieldPassword);
		textFieldPassword.setColumns(10);
		
		textFieldConsole = new JTextField();
		textFieldConsole.setBounds(12, 454, 448, 34);
		textFieldConsole.setEditable(false);
		mainPanel.add(textFieldConsole);
		textFieldConsole.setColumns(10);

		JButton btnTest = new JButton("Connect");
		btnTest.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				postgresDb = new PostgresDB();
				postgresDb.connect(textFieldHost.getText(), textFieldPort.getText(), textFieldUsername.getText(),
						String.valueOf(textFieldPassword.getPassword()), "?");
				try {
					listDb.setModel(listToListModel(postgresDb.listDatabases()));
					tabbedPaneSource.setEnabledAt(1, true);
					tabbedPaneSource.setSelectedIndex(1);
					panelDatabase.setEnabled(true);
					textFieldConsole.setText("Conexão estável, favor selecionar um banco de dados.");
				} catch (Exception s) {
					textFieldConsole.setText("Error: banco não encontrado");
				}
			}
		});
		btnTest.setBounds(22, 166, 243, 25);
		panelServer.add(btnTest);

		
		tabbedPaneSource.addTab("Database", null, panelDatabase, null);
		tabbedPaneSource.setEnabledAt(1, false);
		
		panelDatabase.setLayout(null);
		
		JScrollPane scrollPane = new JScrollPane();
		scrollPane.setBounds(23, 39, 239, 234);
		panelDatabase.add(scrollPane);
		
				// JScrollBar scrollBar = new JScrollBar();
				// scrollBar.setBounds(204, 38, 17, 146);
				// panelDatabase.add(scrollBar);
		
				listDb = new JList();
				scrollPane.setViewportView(listDb);
				listDb.addMouseListener(new MouseAdapter() {
					@Override
					public void mouseClicked(MouseEvent e) {
						if (listDb.getSelectedValue() != null) {
						postgresDb.connect(textFieldHost.getText(), textFieldPort.getText(), textFieldUsername.getText(),
								String.valueOf(textFieldPassword.getPassword()), listDb.getSelectedValue().toString());
						tabbedPaneSource.setEnabledAt(2, true);
						tabbedPaneSource.setSelectedIndex(2);
							textFieldConsole.setText("Conexão estável com o banco de dados " + listDb.getSelectedValue().toString());
						}
					}
				});
		listDb.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		listDb.setVisibleRowCount(6);
        listDb.setLayoutOrientation(JList.VERTICAL);

		JLabel lblSelectDb = new JLabel("Select a database");
		lblSelectDb.setBounds(74, 12, 129, 15);
		panelDatabase.add(lblSelectDb);

		JPanel panelTables = new JPanel();

		tabbedPaneSource.addTab("Tables", null, panelTables, null);
		tabbedPaneSource.setEnabledAt(2, false);
		panelTables.setLayout(null);

		JLabel lblTables_1 = new JLabel("Tables");
		lblTables_1.setFont(new Font("Dialog", Font.BOLD, 14));
		lblTables_1.setBounds(111, 12, 70, 15);
		panelTables.add(lblTables_1);
		try {
		} catch (Exception n) {

		}
		
		JScrollPane scrollPane_1 = new JScrollPane();
		scrollPane_1.setBounds(24, 29, 252, 241);
		panelTables.add(scrollPane_1);
		
				final JList listTable = new JList();
				listTable.addMouseListener(new MouseAdapter() {
					@Override
					public void mouseClicked(MouseEvent arg0) {
						try {
							listColumns.setModel(listToListModel(postgresDb.listColumns(listTable.getSelectedValue().toString())));
							textFieldConsole.setText("Selecione as colunas desejadas para Primary Key e Value");
							tabbedPaneSource.setEnabledAt(3, true);
							tabbedPaneSource.setSelectedIndex(3);
							tabbedPaneTarget.setEnabled(true);
							textFieldPrefixo.setEnabled(true);
							listPk.setEnabled(true);
							btnGetSelectedColumnsPK.setEnabled(true);
							listValue.setEnabled(true);
							btnGetSelectedColumnsVALUE.setEnabled(true);
							btnGenerateSql.setEnabled(true);
							
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
				});
				scrollPane_1.setViewportView(listTable);
				listTable.addListSelectionListener(new ListSelectionListener() {
					
					public void valueChanged(ListSelectionEvent e) {
						textFieldConsole.setText("Selecionado tabela "+ listTable.getSelectedValue().toString());
					}
				});
				
					listTable.addFocusListener(new FocusAdapter() {
						@Override
						public void focusGained(FocusEvent e) {
							try {
								listTable.setModel(listToListModel(postgresDb.listTables()));
								textFieldConsole.setText("Selecione uma tabela");
								} catch (Exception t) {
							}
						}
					});
					listTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
				//	listTable.setModel(postgresDb.listTables());

		JPanel panelColumns = new JPanel();
		tabbedPaneSource.addTab("Columns", null, panelColumns, null);
		tabbedPaneSource.setEnabledAt(3, false);
		panelColumns.setLayout(null);

		JLabel lblColumn = new JLabel("Column");
		lblColumn.setBounds(114, 5, 79, 17);
		panelColumns.add(lblColumn);
		lblColumn.setFont(new Font("Dialog", Font.BOLD, 14));
		
		JScrollPane scrollPane_2 = new JScrollPane();
		scrollPane_2.setBounds(25, 21, 251, 242);
		panelColumns.add(scrollPane_2);
		
				scrollPane_2.setViewportView(listColumns);
				listColumns.addFocusListener(new FocusAdapter() {
					@Override
					public void focusGained(FocusEvent e) {
						try {
							listColumns.setModel(listToListModel(postgresDb.listColumns(listTable.getSelectedValue().toString())));
							textFieldConsole.setText("Selecione as colunas desejadas para Primary Key e Value");
						} catch (Exception z) {
						}
					}
				});

		JLabel lblTables = new JLabel("Target");
		lblTables.setBounds(484, -3, 78, 21);
		lblTables.setFont(new Font("Dialog", Font.BOLD, 14));
		mainPanel.add(lblTables);

		
		tabbedPaneTarget.setEnabled(false);
		tabbedPaneTarget.setBounds(348, 22, 293, 323);
		mainPanel.add(tabbedPaneTarget);

		JPanel panelCollection = new JPanel();
		tabbedPaneTarget.addTab("Collection", null, panelCollection, null);
		tabbedPaneTarget.setEnabledAt(0, true);
		panelCollection.setLayout(null);

		JLabel lblProperties = new JLabel("Prefixo chave");
		lblProperties.setFont(new Font("Dialog", Font.BOLD, 12));
		lblProperties.setBounds(12, 9, 103, 26);
		panelCollection.add(lblProperties);
		
		JScrollPane scrollPane_3 = new JScrollPane();
		scrollPane_3.setBounds(58, 36, 199, 90);
		panelCollection.add(scrollPane_3);

		listPk = new JList();
		listPk.setEnabled(false);
		scrollPane_3.setViewportView(listPk);
		listPk.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

		JLabel lblPk = new JLabel("PK");
		lblPk.setBounds(12, 37, 28, 15);
		panelCollection.add(lblPk);
		
		JScrollPane scrollPane_4 = new JScrollPane();
		scrollPane_4.setBounds(58, 166, 199, 82);
		panelCollection.add(scrollPane_4);

		listValue = new JList();
		listValue.setEnabled(false);
		scrollPane_4.setViewportView(listValue);
		listValue.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

		JLabel lblValue = new JLabel("Value");
		lblValue.setBounds(12, 167, 58, 15);
		panelCollection.add(lblValue);

		btnGetSelectedColumnsPK = new JButton("Get selected row");
		btnGetSelectedColumnsPK.setEnabled(false);
		btnGetSelectedColumnsPK.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				DefaultListModel listPkTemp = new DefaultListModel();
				List<String> selecionadosPk = listColumns.getSelectedValuesList();
				for (String n : selecionadosPk) {
					listPkTemp.addElement(n);
				}
				listPk.setModel(listPkTemp);
				textFieldConsole.setText("Selecionado colunas "+ listPkTemp.toString() +" como Primary Keys");
			}
			
		});
		btnGetSelectedColumnsPK.setBounds(58, 134, 199, 25);
		panelCollection.add(btnGetSelectedColumnsPK);

		 btnGetSelectedColumnsVALUE = new JButton("Get selected row");
		btnGetSelectedColumnsVALUE.setEnabled(false);
		btnGetSelectedColumnsVALUE.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				DefaultListModel listValueTemp = new DefaultListModel();
				List<String> selecionadosValues = listColumns.getSelectedValuesList();
				for (String n : selecionadosValues) {
					listValueTemp.addElement(n);
				}
				listValue.setModel(listValueTemp);
				textFieldConsole.setText("Selecionado colunas "+ listValueTemp.toString() +" como Values");
			}
		});
		btnGetSelectedColumnsVALUE.setBounds(58, 259, 199, 25);
		panelCollection.add(btnGetSelectedColumnsVALUE);

		textFieldPrefixo = new JTextField();
		textFieldPrefixo.setEnabled(false);
		textFieldPrefixo.setBounds(111, 9, 144, 26);
		panelCollection.add(textFieldPrefixo);
		textFieldPrefixo.setColumns(10);
		
		JPanel panelTargetNoSQL = new JPanel();
		panelTargetNoSQL.addComponentListener(new ComponentAdapter() {
			@Override
			public void componentShown(ComponentEvent arg0) {
				comboBoxTargets.addItem(new RedisConnector());
				comboBoxTargets.addItem(new MongoConnector());
			}
		});
		panelTargetNoSQL.setLayout(null);
		tabbedPaneTarget.addTab("NoSQL DB", null, panelTargetNoSQL, null);
		tabbedPaneTarget.setEnabledAt(1, true);
		
		JLabel labelRedis = new JLabel("NoSQL");
		labelRedis.addFocusListener(new FocusAdapter() {
			@Override
			public void focusGained(FocusEvent e) {
				textFieldConsole.setText("Forneça detalhes para conexão com Redis");
			}
		});
		labelRedis.setFont(new Font("Dialog", Font.BOLD, 13));
		labelRedis.setBounds(12, 12, 101, 15);
		panelTargetNoSQL.add(labelRedis);
		
		JLabel labelRedisHost = new JLabel("Host: ");
		labelRedisHost.setBounds(12, 58, 50, 15);
		panelTargetNoSQL.add(labelRedisHost);
		
		JLabel labelRedisPort = new JLabel("Port: ");
		labelRedisPort.setBounds(12, 85, 70, 15);
		panelTargetNoSQL.add(labelRedisPort);
		
		JLabel labelRedisPassword = new JLabel("Password: ");
		labelRedisPassword.setBounds(12, 117, 101, 15);
		panelTargetNoSQL.add(labelRedisPassword);
		
		textFieldRedisHost = new JTextField();
		textFieldRedisHost.setText("localhost");
		textFieldRedisHost.setColumns(10);
		textFieldRedisHost.setBounds(92, 56, 163, 19);
		panelTargetNoSQL.add(textFieldRedisHost);
		
		textFieldRedisPort = new JTextField();
		textFieldRedisPort.setText("6379");
		textFieldRedisPort.setColumns(10);
		textFieldRedisPort.setBounds(92, 83, 163, 19);
		panelTargetNoSQL.add(textFieldRedisPort);
		
		textFieldRedisPassword = new JTextField();
		textFieldRedisPassword.setColumns(10);
		textFieldRedisPassword.setBounds(92, 117, 163, 19);
		panelTargetNoSQL.add(textFieldRedisPassword);
		
		JButton buttonRedisConnect = new JButton("Connect");
		buttonRedisConnect.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				targetDb = (Connector) comboBoxTargets.getSelectedItem();
				
				if (targetDb.connect(textFieldRedisHost.getText().toString(), textFieldRedisPort.getText(), textFieldTargetUser.getText(), textFieldRedisPassword.getText(), textFieldTargetDbName.getText())) {
					btnImport.setEnabled(true);
					textFieldImport.setEnabled(true);
					textFieldConsole.setText("Conexão estável com "+targetDb.toString());
				}else {
					textFieldConsole.setText("Conexão com "+targetDb.toString()+" falhou!");
				}
			}			
		});
		
		buttonRedisConnect.setBounds(12, 251, 243, 25);
		panelTargetNoSQL.add(buttonRedisConnect);
		
		comboBoxTargets = new JComboBox();
		comboBoxTargets.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (comboBoxTargets.getItemCount()>0) {
					if (comboBoxTargets.getSelectedItem() instanceof MongoConnector) {
						textFieldRedisHost.setEnabled(true);
						textFieldRedisPort.setEnabled(true);
						textFieldRedisPassword.setEnabled(true);
						textFieldTargetDbName.setEnabled(true);
						textFieldTargetUser.setEnabled(true);
					}else if (comboBoxTargets.getSelectedItem() instanceof RedisConnector) {
						textFieldRedisHost.setEnabled(true);
						textFieldRedisPort.setEnabled(true);
						textFieldRedisPassword.setEnabled(true);
						textFieldTargetDbName.setEnabled(false);
						textFieldTargetUser.setEnabled(false);
					} 
				}else {
					textFieldRedisHost.setEnabled(false);
					textFieldRedisPort.setEnabled(false);
					textFieldRedisPassword.setEnabled(false);
					textFieldTargetDbName.setEnabled(false);
					textFieldTargetUser.setEnabled(false);
				}
			}
		});
		comboBoxTargets.setBounds(72, 7, 183, 24);
		panelTargetNoSQL.add(comboBoxTargets);
		
		JLabel lblDbName = new JLabel("User");
		lblDbName.setBounds(12, 151, 101, 15);
		panelTargetNoSQL.add(lblDbName);
		
		textFieldTargetUser = new JTextField();
		textFieldTargetUser.setColumns(10);
		textFieldTargetUser.setBounds(92, 151, 163, 19);
		panelTargetNoSQL.add(textFieldTargetUser);
		
		JLabel label = new JLabel("DB name");
		label.setBounds(12, 186, 101, 15);
		panelTargetNoSQL.add(label);
		
		textFieldTargetDbName = new JTextField();
		textFieldTargetDbName.setColumns(10);
		textFieldTargetDbName.setBounds(92, 186, 163, 19);
		panelTargetNoSQL.add(textFieldTargetDbName);

		JPanel panelExecute = new JPanel();
		panelExecute.setBounds(0, 349, 465, 83);
		mainPanel.add(panelExecute);
		panelExecute.setLayout(null);

		JLabel lblExecute = new JLabel("Execute");
		lblExecute.setFont(new Font("Dialog", Font.BOLD, 14));
		lblExecute.setBounds(12, 3, 70, 20);
		panelExecute.add(lblExecute);

		JLabel lblStatus = new JLabel("Source SQL:");
		lblStatus.setBounds(12, 25, 85, 15);
		panelExecute.add(lblStatus);

		textFieldImport = new JTextField();
		textFieldImport.setEnabled(false);
		textFieldImport.setBounds(12, 42, 448, 34);
		panelExecute.add(textFieldImport);
		textFieldImport.setColumns(10);

		btnGenerateSql = new JButton("Generate sql");
		btnGenerateSql.setEnabled(false);
		btnGenerateSql.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String tempPk = "";
				String tempPkVirgula = "";
				for (int x = 0; x < listPk.getModel().getSize(); x++) {
					if (x == 0) {
						tempPk = tempPk + listPk.getModel().getElementAt(x) + "::text";
						tempPkVirgula = tempPkVirgula + listPk.getModel().getElementAt(x);
					} else {
						tempPk = tempPk + "||'_'||" + listPk.getModel().getElementAt(x) + "::text";
						tempPkVirgula = tempPkVirgula + ", " + listPk.getModel().getElementAt(x);
					}
				}
				String tempValue = "";
				for (int x = 0; x < listValue.getModel().getSize(); x++) {
					if (x == 0) {
						tempValue = tempValue + listValue.getModel().getElementAt(x);
					} else {
						tempValue = tempValue + ", " + listValue.getModel().getElementAt(x);
					}
				}
				textFieldImport.setText("SELECT '" + textFieldPrefixo.getText() + "_'||" + tempPk
						+ ", (SELECT row_to_json(_) FROM (SELECT " + tempValue + ") AS _ ) AS VALUE FROM "
						+ listTable.getSelectedValue().toString());
				textFieldConsole.setText("Gerado comando SQL para importação de dados");
			}		
		});

		btnGenerateSql.setBounds(185, 7, 126, 25);
		panelExecute.add(btnGenerateSql);

		JButton btnCancel = new JButton("Cancel");
		btnCancel.setEnabled(false);
		btnCancel.setBounds(472, 458, 117, 25);
		btnCancel.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		mainPanel.add(btnCancel);
		
		
		
		JLabel lblConsole = new JLabel("Console:");
		lblConsole.setBounds(12, 439, 70, 15);
		mainPanel.add(lblConsole);
		
				btnImport = new JButton("Import");
				btnImport.setEnabled(false);
				btnImport.setBounds(472, 395, 117, 25);
				mainPanel.add(btnImport);
				
				btnImport.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						 try {
						 postgresDb.exportRelationalDataToNoSQL(textFieldImport.getText().toString(), targetDb);
						 textFieldConsole.setText("Importado dados para Redis com sucesso");
						 } catch (SQLException e1) {
							 textFieldConsole.setText("Error SQL Exception");
						 } catch (JedisConnectionException e2) {
							 textFieldConsole.setText("Favor inserir um host valido para banco Redis");
						 }
						 
					}
				});

				JButton btnRedisData = new JButton("Redis data");
				btnRedisData.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						redisDb = new RedisConnector();
						redisDb.readRedis();
					}
				});
				btnRedisData.setBounds(472, 357, 117, 25);
				mainPanel.add(btnRedisData);
	}
}
