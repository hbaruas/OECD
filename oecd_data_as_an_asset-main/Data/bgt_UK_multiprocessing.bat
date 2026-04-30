:: V:\DIT_VALUEOFDATA\dit_valueofdata\dit_env\Scripts\activate
:: kinit


:::::::::::::::::::::::::::::::::::::   ONLY FOR GPU RUN  :::::::::::::::::::::::::::::::::::::::::::::::::::::::::
::for /l %%p in (1,1,52) do START V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\optimisedCodeGPU.py %%p
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 36 


:::::::::::::::::::::::::::::::::::::  ONLY FOR ASGENRUN RUN:::::::::::::::::::::::::::::::::::::
::for /l %%p in (0,1,4) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p 
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 5
::for /l %%p in (6,1,10) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p  
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 11
::for /l %%p in (12,1,16) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p  
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 17
::for /l %%p in (18,1,22) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p  
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 23
::for /l %%p in (24,1,28) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p 
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 29
::for /l %%p in (30,1,35) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p

:: stop it for a break cause you will need to relogin to the asgen just by writing kinit into the cmd and your password
:: because even though it runs it will not save after it finishes running
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 36
::for /l %%p in (37,1,41) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p 
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 42
::for /l %%p in (43,1,46) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p 
::START /W python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py 47
::for /l %%p in (48,1,52) do START python V:\DIT_VALUEOFDATA\dit_valueofdata\DIT_internal\timeseries\Updated_files\bgt_GBR_gen_noun_chunks.py %%p 
