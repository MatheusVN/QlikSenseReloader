INSERT INTO 
    logQuebraQlik (
        evento,
        statusQuebra,
        codigoStatus,
        dataQuebra,
        IDCompleto,
        caminhoLog
        )
VALUES
    (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
    );
