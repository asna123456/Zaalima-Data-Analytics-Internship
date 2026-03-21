import matplotlib.pyplot as plt
import seaborn as sns

# Dataset-ile numeric columns mathram edukkunnu
# 'Monetary' ivide 'Sum of Monetary' aayi thanne Python-ilekku varum
df = dataset[['R_Score', 'F_Score', 'M_Score', 'Monetary']]
correlation_matrix = df.corr()

plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Customer Behavior Correlation')
plt.show()