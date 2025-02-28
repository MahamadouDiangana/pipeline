# Define the schema for the DataFrame

schema = StructType([
 StructField('code_departement', StringType(), True),
 StructField('libelle_departement', StringType(), True),
 StructField('code_circonscription', IntegerType(), True),
 StructField('libelle_circonscription', StringType(), True),
 StructField('code_commune', IntegerType(), True),
 StructField('libelle_commune', StringType(), True),
 StructField('code_bur_vote', StringType(), True),
 StructField('inscrits', IntegerType(), True),
 StructField('abstentions', IntegerType(), True),
 StructField('pct_abs_sur_ins', FloatType(), True),
 StructField('votants', IntegerType(), True),
 StructField('pct_vot_sur_ins', FloatType(), True),
 StructField('blancs', StringType(), True),
 StructField('pct_blancs_sur_ins', FloatType(), True),
 StructField('pct_blancs_sur_vot', FloatType(), True),
 StructField('nuls', IntegerType(), True),
 StructField('pct_nuls_sur_ins', FloatType(), True),
 StructField('pct_nuls_sur_vot', FloatType(), True),
 StructField('exprimes', IntegerType(), True),
 StructField('pct_exprimes_sur_ins', FloatType(), True),
 StructField('pct_exprimes_sur_vot', FloatType(), True),
 StructField('num_panneau_ARTHAUD_Nathalie', IntegerType(), True),
 StructField('sex_ARTHAUD_Nathalie', StringType(), True),
 StructField('nom_ARTHAUD_Nathalie', StringType(), True),
 StructField('prenom_ARTHAUD_Nathalie', StringType(), True),
 StructField('voix_ARTHAUD_Nathalie', IntegerType(), True),
 StructField('pct_voix_sur_ins_ARTHAUD_Nathalie', FloatType(), True),
 StructField('pct_voix_sur_exp_ARTHAUD_Nathalie', FloatType(), True),
 StructField('num_panneau_ROUSSEL_Fabien', IntegerType(), True),
 StructField('sex_ROUSSEL_Fabien', StringType(), True),
 StructField('nom_ROUSSEL_Fabien', StringType(), True),
 StructField('prenom_ROUSSEL_Fabien', StringType(), True),
 StructField('voix_ROUSSEL_Fabien', IntegerType(), True),
 StructField('pct_voix_sur_ins_ROUSSEL_Fabien', FloatType(), True),
 StructField('pct_voix_sur_exp_ROUSSEL_Fabien', FloatType(), True),
 StructField('num_panneau_MACRON_Emmanuel', IntegerType(), True),
 StructField('sex_MACRON_Emmanuel', StringType(), True),
 StructField('nom_MACRON_Emmanuel', StringType(), True),
 StructField('prenom_MACRON_Emmanuel', StringType(), True),
 StructField('voix_MACRON_Emmanuel', IntegerType(), True),
 StructField('pct_voix_sur_ins_MACRON_Emmanuel', FloatType(), True),
 StructField('pct_voix_sur_exp_MACRON_Emmanuel', FloatType(), True),
 StructField('num_panneau_LASSALLE_Jean', IntegerType(), True),
 StructField('sex_LASSALLE_Jean', StringType(), True),
 StructField('nom_LASSALLE_Jean', StringType(), True),
 StructField('prenom_LASSALLE_Jean', StringType(), True),
 StructField('voix_LASSALLE_Jean', IntegerType(), True),
 StructField('pct_voix_sur_ins_LASSALLE_Jean', FloatType(), True),
 StructField('pct_voix_sur_exp_LASSALLE_Jean', FloatType(), True),
 StructField('num_panneau_LE_PEN_Marine', IntegerType(), True),
 StructField('sex_LE_PEN_Marine', StringType(), True),
 StructField('nom_LE_PEN_Marine', StringType(), True),
 StructField('prenom_LE_PEN_Marine', StringType(), True),
 StructField('voix_LE_PEN_Marine', IntegerType(), True),
 StructField('pct_voix_sur_ins_LE_PEN_Marine', FloatType(), True),
 StructField('pct_voix_sur_exp_LE_PEN_Marine', FloatType(), True),
 StructField('num_panneau_ZEMMOUR_Eric', IntegerType(), True),
 StructField('sex_ZEMMOUR_Eric', StringType(), True),
 StructField('nom_ZEMMOUR_Eric', StringType(), True),
 StructField('prenom_ZEMMOUR_Eric', StringType(), True),
 StructField('voix_ZEMMOUR_Eric', IntegerType(), True),
 StructField('pct_voix_sur_ins_ZEMMOUR_Eric', FloatType(), True),
 StructField('pct_voix_sur_exp_ZEMMOUR_Eric', FloatType(), True),
 StructField('num_panneau_MELENCHON_Jean_Luc', IntegerType(), True),
 StructField('sex_MELENCHON_Jean_Luc', StringType(), True),
 StructField('nom_MELENCHON_Jean_Luc', StringType(), True),
 StructField('prenom_MELENCHON_Jean_Luc', StringType(), True),
 StructField('voix_MELENCHON_Jean_Luc', IntegerType(), True),
 StructField('pct_voix_sur_ins_MELENCHON_Jean_Luc', FloatType(), True),
 StructField('pct_voix_sur_exp_MELENCHON_Jean_Luc', FloatType(), True),
 StructField('num_panneau_HIDALGO_Anne', IntegerType(), True),
 StructField('sex_HIDALGO_Anne', StringType(), True),
 StructField('nom_HIDALGO_Anne', StringType(), True),
 StructField('prenom_HIDALGO_Anne', StringType(), True),
 StructField('voix_HIDALGO_Anne', IntegerType(), True),
 StructField('pct_voix_sur_ins_HIDALGO_Anne', FloatType(), True),
 StructField('pct_voix_sur_exp_HIDALGO_Anne', FloatType(), True),
 StructField('num_panneau_JADOT_Yannick', IntegerType(), True),
 StructField('sex_JADOT_Yannick', StringType(), True),
 StructField('nom_JADOT_Yannick', StringType(), True),
 StructField('prenom_JADOT_Yannick', StringType(), True),
 StructField('voix_JADOT_Yannick', IntegerType(), True),
 StructField('pct_voix_sur_ins_JADOT_Yannick', FloatType(), True),
 StructField('pct_voix_sur_exp_JADOT_Yannick', FloatType(), True),
 StructField('num_panneau_PECRESSE_Valerie', IntegerType(), True),
 StructField('sex_PECRESSE_Valerie', StringType(), True),
 StructField('nom_PECRESSE_Valerie', StringType(), True),
 StructField('prenom_PECRESSE_Valerie', StringType(), True),
 StructField('voix_PECRESSE_Valerie', IntegerType(), True),
 StructField('pct_voix_sur_ins_PECRESSE_Valerie', FloatType(), True),
 StructField('pct_voix_sur_exp_PECRESSE_Valerie', FloatType(), True),
 StructField('num_panneau_POUTOU_Philippe', IntegerType(), True),
 StructField('sex_POUTOU_Philippe', StringType(), True),
 StructField('nom_POUTOU_Philippe', StringType(), True),
 StructField('prenom_POUTOU_Philippe', StringType(), True),
 StructField('voix_POUTOU_Philippe', IntegerType(), True),
 StructField('pct_voix_sur_ins_POUTOU_Philippe', FloatType(), True),
 StructField('pct_voix_sur_exp_POUTOU_Philippe', FloatType(), True),
 StructField('num_panneau_DUPONT_AIGNAN_Nicolas', IntegerType(), True),
 StructField('sex_DUPONT_AIGNAN_Nicolas', StringType(), True),
 StructField('nom_DUPONT_AIGNAN_Nicolas', StringType(), True),
 StructField('prenom_DUPONT_AIGNAN_Nicolas', StringType(), True),
 StructField('voix_DUPONT_AIGNAN_Nicolas', IntegerType(), True),
 StructField('pct_voix_sur_ins_DUPONT_AIGNAN_Nicolas', FloatType(), True),
 StructField('pct_voix_sur_exp_DUPONT_AIGNAN_Nicolas', FloatType(), True)
 ])
